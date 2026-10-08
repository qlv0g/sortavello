import os
import sys
import shutil
import time
import heapq
import tempfile
import argparse
from contextlib import ExitStack
from concurrent.futures import ProcessPoolExecutor, as_completed
from termcolor import colored

TEMP_DIR = os.environ.get('HCAPTCHA_TEMP') or os.path.join(os.getcwd(), 'TEMP')
os.makedirs(TEMP_DIR, exist_ok=True)

MiB = 1024 * 1024
READ_BLOCK = 8 * MiB
FILE_BUF = 4 * MiB
MERGE_READ_BUF = 512 * 1024
WRITE_BATCH_LINES = 16 * 1024
MERGE_WRITE_BATCH = 64 * 1024
BOUNDARY_PROBE = 4 * 1024
BOUNDARY_MAX_SCAN = 16 * MiB
CHUNK_MIN = 8 * MiB
CHUNK_MAX = 64 * MiB
RAM_BUDGET_FACTOR = 0.45
WORKER_CHUNK_RAM_FACTOR = 3.6
FAST_PATH_FACTOR = 0.10
FAST_PATH_MAX = 384 * MiB
DUP_STRATEGY_THRESHOLD = 0.2
SAMPLE_BLOCKS = 8
SAMPLE_BLOCK_LINES = 4096

def get_ram():
    try:
        if os.name == 'nt':
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ('dwLength', ctypes.c_ulong),
                    ('dwMemoryLoad', ctypes.c_ulong),
                    ('ullTotalPhys', ctypes.c_ulonglong),
                    ('ullAvailPhys', ctypes.c_ulonglong),
                    ('ullTotalPageFile', ctypes.c_ulonglong),
                    ('ullAvailPageFile', ctypes.c_ulonglong),
                    ('ullTotalVirtual', ctypes.c_ulonglong),
                    ('ullAvailVirtual', ctypes.c_ulonglong),
                    ('ullAvailExtendedVirtual', ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                return max(1, stat.ullTotalPhys // MiB)
        else:
            pages = os.sysconf('SC_PHYS_PAGES')
            page_size = os.sysconf('SC_PAGE_SIZE')
            return max(1, pages * page_size // MiB)
    except Exception:
        pass
    return 4096

def get_cpu_count():
    return os.cpu_count() or 2

def calc_chunk_size(workers, ram, size):
    by_parallel = size / max(1, workers)
    by_ram = ram * MiB * RAM_BUDGET_FACTOR / max(1, workers) / WORKER_CHUNK_RAM_FACTOR
    return int(min(max(min(by_parallel, by_ram), CHUNK_MIN), CHUNK_MAX))

def calc_fan_in(ram):
    if ram < 4096:
        return 64
    if ram < 8192:
        return 128
    return 256

def run_jobs(pool, func, jobs, msg):
    res = []
    n = len(jobs)
    last = time.time()
    if pool is None:
        for i, job in enumerate(jobs, 1):
            res.append(func(job))
            now = time.time()
            if i == n or i % 10 == 0 or now - last > 5.0:
                print(colored(f"{msg}: {i}/{n} ({i * 100 // n}%)", "cyan"))
                last = now
        return res

    futs = [pool.submit(func, job) for job in jobs]
    for i, fut in enumerate(as_completed(futs), 1):
        res.append(fut.result())
        now = time.time()
        if i == n or i % 10 == 0 or now - last > 5.0:
            print(colored(f"{msg}: {i}/{n} ({i * 100 // n}%)", "cyan"))
            last = now
    return res

def calc_chunk_ranges(path, chunk):
    size = os.path.getsize(path)
    if size <= 0:
        return []
    starts = [0]
    pos = chunk
    with open(path, 'rb') as f:
        while pos < size:
            f.seek(pos)
            found = None
            scanned = 0
            while scanned < BOUNDARY_MAX_SCAN:
                buf = f.read(BOUNDARY_PROBE)
                if not buf:
                    break
                nl = buf.find(b'\n')
                if nl >= 0:
                    found = pos + scanned + nl + 1
                    break
                scanned += len(buf)
            if found is None or found >= size:
                break
            starts.append(found)
            pos = found + chunk
    return [(starts[i], starts[i + 1] if i + 1 < len(starts) else size)
            for i in range(len(starts))]

def calc_dup_ratio(lines):
    n = len(lines)
    if n <= SAMPLE_BLOCKS * SAMPLE_BLOCK_LINES:
        sample = lines
    else:
        sample = []
        stride = n // SAMPLE_BLOCKS
        for i in range(SAMPLE_BLOCKS):
            sample.extend(lines[i * stride:i * stride + SAMPLE_BLOCK_LINES])
    if not sample:
        return 0.0
    return 1.0 - len(set(sample)) / len(sample)

def save_sorted(lines, out, force_set=False):
    total = len(lines)

    if force_set or calc_dup_ratio(lines) >= DUP_STRATEGY_THRESHOLD:
        uniq = set(lines)
        del lines
        kept = 0
        batch = []
        for line in sorted(uniq):
            batch.append(line)
            kept += 1
            if len(batch) >= WRITE_BATCH_LINES:
                out.write(b'\n'.join(batch))
                out.write(b'\n')
                batch.clear()
        if batch:
            out.write(b'\n'.join(batch))
            out.write(b'\n')
        return kept, total - kept

    lines.sort()
    kept = 0
    prev = None
    batch = []
    for line in lines:
        if line == prev:
            continue
        prev = line
        kept += 1
        batch.append(line)
        if len(batch) >= WRITE_BATCH_LINES:
            out.write(b'\n'.join(batch))
            out.write(b'\n')
            batch.clear()
    if batch:
        out.write(b'\n'.join(batch))
        out.write(b'\n')
    return kept, total - kept

def process_chunk(job):
    path, start, end, temp_dir = job
    lines = []
    leftover = b''
    left = end - start
    name = None

    try:
        with open(path, 'rb', buffering=MERGE_READ_BUF) as f:
            f.seek(start)
            while left > 0:
                block = f.read(min(READ_BLOCK, left))
                if not block:
                    break
                left -= len(block)
                if leftover:
                    block = leftover + block
                    leftover = b''
                parts = block.split(b'\n')
                if left > 0:
                    leftover = parts.pop()
                elif parts and parts[-1] == b'':
                    parts.pop()
                lines.extend(parts)
            if leftover:
                lines.append(leftover)
    except Exception as e:
        print(colored(f"Chunk read error [{start}:{end}]: {e}", "red"))
        return None, 0, 0, 0

    if not lines:
        return None, 0, 0, 0

    total = len(lines)
    try:
        with tempfile.NamedTemporaryFile(delete=False, dir=temp_dir,
                                         suffix='.run', prefix='run_') as f:
            name = f.name
            _, dup = save_sorted(lines, f)
        return name, total, dup, os.path.getsize(name)
    except Exception as e:
        print(colored(f"Chunk write error [{start}:{end}]: {e}", "red"))
        if name:
            try:
                os.remove(name)
            except OSError:
                pass
        return None, 0, 0, 0

def merge_runs(job):
    files, temp_dir, out_path = job
    unique = 0
    dup = 0
    created = None

    try:
        if out_path is None:
            with tempfile.NamedTemporaryFile(delete=False, dir=temp_dir,
                                             suffix='.run', prefix='run_') as t:
                out_path = t.name
                created = out_path
        with ExitStack() as stack:
            iters = [stack.enter_context(open(p, 'rb', buffering=MERGE_READ_BUF))
                     for p in files]
            out = stack.enter_context(open(out_path, 'wb', buffering=FILE_BUF))
            buf = []
            prev = None
            for line in heapq.merge(*iters):
                if line != prev:
                    prev = line
                    buf.append(line)
                    unique += 1
                    if len(buf) >= MERGE_WRITE_BATCH:
                        out.write(b''.join(buf))
                        buf.clear()
                else:
                    dup += 1
            if buf:
                out.write(b''.join(buf))
        return out_path, unique, dup, True
    except Exception as e:
        print(colored(f"Merge error: {e}", "red"))
        if created:
            try:
                os.remove(created)
            except OSError:
                pass
        return None, 0, 0, False

def sort_in_memory(infile, outfile):
    lines = []
    leftover = b''
    with open(infile, 'rb', buffering=FILE_BUF) as f:
        while True:
            block = f.read(READ_BLOCK)
            if not block:
                break
            if leftover:
                block = leftover + block
                leftover = b''
            parts = block.split(b'\n')
            leftover = parts.pop()
            lines.extend(parts)
        if leftover:
            lines.append(leftover)

    total = len(lines)
    if not total:
        open(outfile, 'wb').close()
        return 0, 0

    with open(outfile, 'wb', buffering=FILE_BUF) as f:
        _, dup = save_sorted(lines, f)
    return total, dup

def sort_and_uniq(infile, outfile, workers, chunk, fan):
    size = os.path.getsize(infile)
    ram = get_ram()

    if chunk <= 0:
        chunk = calc_chunk_size(workers, ram, size)
    if fan <= 0:
        fan = calc_fan_in(ram)
    else:
        fan = max(2, fan)

    try:
        free = shutil.disk_usage(TEMP_DIR).free / (1024 ** 3)
    except OSError:
        free = -1

    print(colored(f"Input: {infile} ({size // MiB} MiB), ram {ram} MB, "
                  f"workers {workers}, chunk {chunk // MiB} MiB, fan-in {fan}", "cyan"))
    if free >= 0:
        print(colored(f"Temp dir: {TEMP_DIR} (free {round(free, 1)} GiB)", "cyan"))
        if size > 0 and free < size / (1024 ** 3):
            print(colored("Not enough free space in TEMP, "
                          "set HCAPTCHA_TEMP to another disk", "yellow"))
    else:
        print(colored(f"Temp dir: {TEMP_DIR}", "cyan"))

    t0 = time.time()

    if size <= min(ram * MiB * FAST_PATH_FACTOR, FAST_PATH_MAX):
        print(colored(f"In-memory sort ({size // MiB} MiB fits in ram)", "cyan"))
        read_n, dupes = sort_in_memory(infile, outfile)
        print(colored(f"Done: read {read_n}, unique {read_n - dupes}, "
                      f"dupes {dupes}, {round(time.time() - t0, 2)}s", "green"))
        return read_n

    ranges = calc_chunk_ranges(infile, chunk)
    jobs = [(infile, s, e, TEMP_DIR) for s, e in ranges]
    print(colored(f"Chunk ranges: {len(jobs)}", "cyan"))

    pool = None
    if workers > 1:
        pool = ProcessPoolExecutor(max_workers=workers)

    runs = []
    run_dupes = 0
    run_lines = 0
    run_bytes = 0

    try:
        res = run_jobs(pool, process_chunk, jobs, "Chunk sort")
        bad = sum(1 for r in res if r[0] is None)
        for name, cnt, dup, written in res:
            if name:
                runs.append(name)
                run_bytes += written
            run_dupes += dup
            run_lines += cnt
        if bad:
            raise Exception(f"{bad} chunk(s) failed, temp files in {TEMP_DIR}")
        print(colored(f"Chunks done: {len(runs)} runs, {run_lines} lines, "
                      f"{run_bytes // MiB} MiB, in-chunk dupes: {run_dupes}", "cyan"))


        merge_dupes = 0
        pass_no = 0
        out_ready = False

        while len(runs) > 1:
            pass_no += 1
            t_pass = time.time()
            if len(runs) <= fan:
                print(colored(f"Final merge: {len(runs)} runs -> {outfile}", "cyan"))
                out_path, unique, dup, ok = merge_runs((runs, TEMP_DIR, outfile))
                if not ok:
                    raise Exception("final merge failed")
                merge_dupes += dup
                for tf in runs:
                    try:
                        os.remove(tf)
                    except OSError as e:
                        print(colored(f"Can't remove {tf}: {e}", "yellow"))
                runs = []
                out_ready = True
                print(colored(f"Final merge: {unique} unique lines, {dup} dupes, "
                              f"{round(time.time() - t_pass, 2)}s", "cyan"))
            else:
                batches = [runs[i:i + fan] for i in range(0, len(runs), fan)]
                print(colored(f"Merge pass {pass_no}: {len(runs)} runs -> "
                              f"{len(batches)} batches", "cyan"))
                mjobs = [(b, TEMP_DIR, None) for b in batches]
                mres = run_jobs(pool, merge_runs, mjobs, f"Merge pass {pass_no}")
                new_runs = []
                pass_dup = 0
                for out_path, unique, dup, ok in mres:
                    if not ok:
                        raise Exception(f"merge pass {pass_no} failed")
                    new_runs.append(out_path)
                    pass_dup += dup
                for b, _, _ in mjobs:
                    for tf in b:
                        try:
                            os.remove(tf)
                        except OSError as e:
                            print(colored(f"Can't remove {tf}: {e}", "yellow"))
                runs = new_runs
                merge_dupes += pass_dup
                print(colored(f"Merge pass {pass_no} done: {pass_dup} dupes, "
                              f"{round(time.time() - t_pass, 2)}s", "cyan"))

        if len(runs) == 1:
            try:
                os.replace(runs[0], outfile)
            except OSError:
                shutil.move(runs[0], outfile)
            runs = []
            out_ready = True
        elif not runs and not out_ready:
            open(outfile, 'wb').close()
    finally:
        if pool is not None:
            pool.shutdown(wait=True)

    total_dupes = run_dupes + merge_dupes
    print(colored(f"Done: read {run_lines}, unique {run_lines - total_dupes}, "
                  f"dupes {total_dupes} ({run_dupes} in-chunk + "
                  f"{merge_dupes} on merge), {round(time.time() - t0, 2)}s", "green"))
    return run_lines

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="sort + unique для больших файлов")
    parser.add_argument('input', nargs='?', default='large_random_emails.txt',
                        help="входной файл")
    parser.add_argument('output', nargs='?', default='output-sorted-unique.txt',
                        help="выходной файл (сортированный, без дублей)")
    parser.add_argument('--workers', type=int, default=0,
                        help="число процессов (0 = все ядра)")
    parser.add_argument('--chunk-mb', type=float, default=0,
                        help="размер чанка в MiB (0 = авто)")
    parser.add_argument('--fan-in', type=int, default=0,
                        help="файлов в merge-проходе (0 = авто)")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(colored(f"File not found: {args.input}", "red"))
        sys.exit(2)
    if os.path.abspath(args.input) == os.path.abspath(args.output):
        print(colored("Input and output must be different files", "red"))
        sys.exit(2)

    workers = args.workers if args.workers > 0 else get_cpu_count()
    chunk = int(args.chunk_mb * MiB) if args.chunk_mb > 0 else 0

    t0 = time.time()
    try:
        sort_and_uniq(args.input, args.output, workers, chunk, args.fan_in)
    except KeyboardInterrupt:
        print(colored("Interrupted", "yellow"))
        sys.exit(130)
    except Exception as e:
        print(colored(f"Error: {e}", "red"))
        sys.exit(1)
    print(colored(f"Total: {round(time.time() - t0, 3)}s", "green"))
