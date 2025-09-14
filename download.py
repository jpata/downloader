#!/usr/bin/env python3

import argparse
import getpass
import os
import stat
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import paramiko
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

# --- Global thread-safe dictionary to store worker status ---
worker_status = {}
status_lock = threading.Lock()

# --- Custom Exception for Timeout ---
class ProgressTimeout(Exception):
    """Custom exception raised when a file transfer has no progress."""
    pass

# --- Class to monitor progress for each download ---
class TransferMonitor:
    """
    A thread-safe class to monitor the state of a single file transfer.
    It holds the state that is shared between the download thread and its watchdog thread.
    """
    def __init__(self, worker_id, filename, total_size):
        self.worker_id = worker_id
        self.filename = filename
        self.total_size = total_size
        self._lock = threading.Lock()
        
        # Shared state variables
        self.last_activity_time = time.time()
        self.download_complete = False
        self.timed_out = False

        self._update_ui_status(0, 0)

    def _update_ui_status(self, transferred, speed):
        """Safely updates the global worker_status dictionary for the UI."""
        with status_lock:
            worker_status[self.worker_id] = {
                "filename": self.filename,
                "transferred": transferred,
                "total": self.total_size,
                "speed": speed,
                "last_activity_ts": time.time(),
            }
            
    def progress_callback(self, bytes_transferred, _):
        """The callback function passed to paramiko's sftp.get()."""
        with self._lock:
            self.last_activity_time = time.time()
        
        # Calculate speed based on total time to provide a more stable average
        elapsed = time.time() - self.start_time
        speed = bytes_transferred / elapsed if elapsed > 0 else 0
        self._update_ui_status(bytes_transferred, speed)

    def set_start_time(self):
        self.start_time = time.time()

# --- Download Worker Function (runs in a thread) ---
def download_file_worker(worker_id, conn_details, remote_path, local_path, timeout):
    """
    Connects and downloads a single file. Includes a dedicated monitor 
    thread to enforce the inactivity timeout.
    """
    ssh_client = None
    monitor_thread = None
    
    try:
        with paramiko.SSHClient() as ssh_pre:
            ssh_pre.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_pre.connect(**conn_details, timeout=10)
            with ssh_pre.open_sftp() as sftp_pre:
                total_size = sftp_pre.stat(remote_path).st_size
        monitor = TransferMonitor(worker_id, os.path.basename(remote_path), total_size)
    except Exception as e:
        raise IOError(f"Failed to get file info for {os.path.basename(remote_path)}: {e}") from e

    def watchdog():
        while True:
            with monitor._lock:
                if monitor.download_complete: break
                stalled_time = time.time() - monitor.last_activity_time
            if stalled_time > timeout:
                with monitor._lock: monitor.timed_out = True
                if ssh_client: ssh_client.get_transport().close()
                break
            time.sleep(1)

    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(**conn_details, timeout=20)
        
        monitor.set_start_time()
        monitor_thread = threading.Thread(target=watchdog, daemon=True)
        monitor_thread.start()

        with ssh_client.open_sftp() as sftp_client:
            sftp_client.get(remote_path, local_path, callback=monitor.progress_callback)

    except Exception as e:
        with monitor._lock:
            if monitor.timed_out:
                raise ProgressTimeout(f"No progress for over {timeout} seconds.") from e
        raise e
        
    finally:
        with monitor._lock: monitor.download_complete = True
        if monitor_thread: monitor_thread.join()
        if os.path.exists(local_path) and not monitor.download_complete:
             os.remove(local_path)
        if ssh_client: ssh_client.close()

# --- UI Generation ---
def generate_ui(overall_progress, total_files_to_process, success, failed):
    """Generates the rich renderable for the live display."""
    overall_progress.update(
        0, completed=success + failed,
        description=f"[bold green]{success} successful[/] | [bold red]{failed} failed[/]"
    )
    worker_table = Table.grid(expand=True)
    worker_table.add_column("ID", justify="center", style="cyan", no_wrap=True)
    worker_table.add_column("File", style="magenta")
    worker_table.add_column("Progress")
    worker_table.add_column("Speed", style="yellow")
    worker_table.add_column("Activity", style="dim")

    with status_lock:
        active_workers = dict(worker_status)

    now = time.time()
    for worker_id, status in active_workers.items():
        filename, transferred, total = status.get("filename", "N/A"), status.get("transferred", 0), status.get("total", 1)
        speed_bps, last_activity_ts = status.get("speed", 0), status.get("last_activity_ts", now)
        if speed_bps > 1024 * 1024: speed = f"{speed_bps / (1024*1024):.2f} MB/s"
        elif speed_bps > 1024: speed = f"{speed_bps / 1024:.1f} KB/s"
        else: speed = f"{speed_bps:.0f} B/s"
        progress_bar = Progress(TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), BarColumn(bar_width=None), expand=True)
        progress_bar.add_task("download", total=total, completed=transferred)
        worker_table.add_row(f"[bold cyan]{worker_id}", filename, progress_bar, speed, f"{now - last_activity_ts:.1f}s ago")
        
    return Panel(worker_table, title="[bold]Active Transfers[/]", border_style="blue", subtitle=f"[dim]Files to Process: {total_files_to_process}[/]")

# --- Main Function ---
def main():
    parser = argparse.ArgumentParser(description="Parallel SFTP file downloader with a rich terminal UI.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("host", help="Remote server hostname or IP address.")
    parser.add_argument("remote_dir", help="Remote directory to download files from.")
    parser.add_argument("local_dir", help="Local directory to save files to.")
    parser.add_argument("-u", "--user", required=True, help="SSH username.")
    parser.add_argument("-k", "--key-file", help="Path to private SSH key file.")
    parser.add_argument("-p", "--port", type=int, default=22, help="SSH port.")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of parallel workers.")
    parser.add_argument("-t", "--timeout", type=int, default=30, help="Inactivity timeout in seconds.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files that already exist locally.")
    args = parser.parse_args()

    console = Console()
    password = None
    if not args.key_file: password = getpass.getpass(f"Password for {args.user}@{args.host}: ")
    os.makedirs(args.local_dir, exist_ok=True)
    conn_details = {"hostname": args.host, "port": args.port, "username": args.user, "password": password, "key_filename": args.key_file}

    all_remote_files = []
    try:
        with console.status("[bold yellow]Connecting to retrieve file list..."):
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(**conn_details)
                with ssh.open_sftp() as sftp:
                    for attr in sftp.listdir_attr(args.remote_dir):
                        if stat.S_ISREG(attr.st_mode): all_remote_files.append(attr.filename)
    except Exception as e:
        console.print(f"🚨 [bold red]Error:[/bold red] Could not list remote files. Reason: {e}"); sys.exit(1)

    if not all_remote_files: console.print("[yellow]No files found in the remote directory. Exiting.[/]"); return
    
    # ⭐ NEW: Filter files if --skip-existing is used
    files_to_process = []
    skipped_count = 0
    if args.skip_existing:
        console.print("[yellow]Checking for existing files to skip...[/yellow]")
        for filename in all_remote_files:
            full_filename = os.path.join(args.local_dir, filename)
            if os.path.exists(full_filename) and os.path.getsize(full_filename)>0:
                skipped_count += 1
            else:
                files_to_process.append(filename)
    else:
        files_to_process = all_remote_files

    if skipped_count > 0:
        console.print(f"👍 [cyan]Skipped {skipped_count} files that already exist locally.[/cyan]")
        
    if not files_to_process:
        console.print("✅ [green]All remote files already exist locally. Nothing to do.[/green]"); return
        
    total_files_to_process = len(files_to_process)
    console.print(f"Found {len(all_remote_files)} total files. [bold]Queuing {total_files_to_process} for download[/bold] with {args.workers} workers.")

    overall_progress = Progress(SpinnerColumn(), TextColumn("[bold blue]Downloading..."), BarColumn(), TextColumn("{task.completed}/{task.total}"), TimeElapsedColumn(), TextColumn("{task.description}"))
    overall_progress.add_task("All files", total=total_files_to_process)
    success_count, failure_count = 0, 0

    with Live(overall_progress, console=console, vertical_overflow="visible", auto_refresh=False) as live:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_task = {}
            for i, filename in enumerate(files_to_process):
                worker_id = f"W{i+1}"; remote_path = f"{args.remote_dir.rstrip('/')}/{filename}"; local_path = os.path.join(args.local_dir, filename)
                future = executor.submit(download_file_worker, worker_id, conn_details, remote_path, local_path, args.timeout)
                future_to_task[future] = {"id": worker_id, "filename": filename}

            while (success_count + failure_count) < total_files_to_process:
                for future in list(future_to_task):
                    if future.done():
                        task = future_to_task.pop(future); worker_id = task["id"]
                        try: future.result(); success_count += 1
                        except Exception: failure_count += 1
                        finally:
                            with status_lock:
                                if worker_id in worker_status: del worker_status[worker_id]
                live.update(generate_ui(overall_progress, total_files_to_process, success_count, failure_count), refresh=True)
                time.sleep(0.1)
        live.update(generate_ui(overall_progress, total_files_to_process, success_count, failure_count), refresh=True)

    console.print("\n--- [bold]Download Summary[/] ---")
    console.print(f"Total files on remote: {len(all_remote_files)}")
    if skipped_count > 0:
        console.print(f"👍 [cyan]Skipped (already exist)[/]: {skipped_count}")
    console.print(f"✅ [bold green]Successful downloads[/]: {success_count}/{total_files_to_process}")
    console.print(f"❌ [bold red]Failed/Timed Out[/]: {failure_count}/{total_files_to_process}")

if __name__ == "__main__":
    main()
