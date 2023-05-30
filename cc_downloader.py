import json
import sys
import argparse
import subprocess
import queue
import threading
import logging

url_path_queue = queue.Queue()
download_threads = []
download_history = dict()
lock = threading.Lock()


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

def write_history(history_path: str) -> None:
    global download_history
    with open(history_path, 'w') as output_history:
        json.dump(download_history, fp=output_history, indent=4)
    output_history.close()

def wget_url(url_path: str, output_prefix: str) -> bool:
    base_url = 'https://data.commoncrawl.org/'
    complete_url = '%s%s' % (base_url, url_path)
    wget_cmd = 'wget -t 3 -c -P %s %s' % (output_prefix, complete_url)
    wget_complete = subprocess.run(wget_cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    if wget_complete.returncode != 0:
        return False
    return True

def download_worker(thread_id: int, output_prefix: str, history_path: str) -> None:
    global download_history
    task_counter = 0
    while True:
        url_task = url_path_queue.get()
        if url_task is None:
            break
        if url_task in download_history:
            continue
        task_counter += 1
        log.info('thread %d start downloading %s' % (thread_id, url_task))
        if not wget_url(url_task, output_prefix):
            log.info('thread %d fail to download %s' % (thread_id, url_task))
            continue
        log.info('thread %d finish to download %s' % (thread_id, url_task))
        lock.acquire()
        try:
            # keep finished url in history
            download_history[url_task] = 1
            if task_counter % 2 == 0:
                write_history(history_path)
        finally:
            lock.release()
    return
    

def download_url_paths(path_file: str, history_json: str, output_path: str, num_thread: int) -> bool:
    global download_history
    # load download history
    with open(history_json, 'r') as history_set:
        download_history = json.load(history_set)
    history_set.close()

    #create download thread
    for th_id in range(num_thread):
        thd = threading.Thread(target=download_worker, args=(th_id, output_path, history_json))
        thd.start()
        download_threads.append(thd)
        
    # write url_path into task queue
    with open(path_file, 'r') as path_list:
        for line in path_list:
            url_path_queue.put(line.strip())
    path_list.close()
    
    # stop workers
    for id in range(num_thread):
        url_path_queue.put(None)
    for thd in download_threads:
        thd.join()

    write_history(history_json)
    return True

def main():
    parser = argparse.ArgumentParser(description='Common Crawl Downloader.')
    parser.add_argument('--urlist', dest='urlist', type=str,  required=True,
                    help='list of url to download')
    parser.add_argument('--history', dest='history', type=str,  required=True,
                    help='download history for resuming')
    parser.add_argument('--thread', dest='thread', type=int,  required=True,
                    help='number of downloading threads')
    parser.add_argument('--output', dest='output', type=str,  required=True,
                    help='output path for downloaded files')
    download_args = parser.parse_args()
    
    # print(download_args.urlist)
    if not download_url_paths(download_args.urlist, download_args.history, download_args.output, download_args.thread):
        return -1
    return 0;


if __name__ == "__main__":
    sys.exit(main())