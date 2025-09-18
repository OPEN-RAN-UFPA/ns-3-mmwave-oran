import csv
from typing import Dict, List, Set, Tuple
import numpy as np
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import threading
import time
from statsd import StatsClient
import re

lock = threading.Lock()


class SimWatcher(PatternMatchingEventHandler):

    patterns = ['cu-up-cell-*.txt', 'cu-cp-cell-*.txt', "du-cell-*.txt"]
    kpm_map: Dict[Tuple[int, int, int], List] = {}
    consumed_keys: Set[Tuple[int, int, int]]
    telegraf_host = "localhost"
    telegraf_port = 8125
    statsd_client = StatsClient(telegraf_host, telegraf_port, prefix=None)

    def __init__(self):
        super().__init__(patterns=self.patterns,
                         ignore_patterns=[],
                         ignore_directories=True,
                         case_sensitive=False)
        self.directory = ''
        self.consumed_keys = set()

    def on_created(self, event):
        super().on_created(event)

    def on_modified(self, event):
        print("Logs modificados\n")
        super().on_modified(event)

        lock.acquire()
        with open(event.src_path, 'r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                timestamp = int(row['timestamp'])
                ue_imsi = int(row['ueImsiComplete'])
                ue = row['ueImsiComplete']

                key = None  # inicializa antes de usar

                if re.search('cu-up-cell-[2-5].txt', file.name):
                    key = (timestamp, ue_imsi, 0)
                elif re.search('cu-cp-cell-[2-5].txt', file.name):
                    key = (timestamp, ue_imsi, 1)
                elif re.search('du-cell-[1-5].txt', file.name):
                    key = (timestamp, ue_imsi, 2)
                elif file.name == './cu-up-cell-1.txt':
                    key = (timestamp, ue_imsi, 3)   # eNB cell
                elif file.name == './cu-cp-cell-1.txt':
                    key = (timestamp, ue_imsi, 4)

                # se não encontrou correspondência, ignora
                if key is None:
                    continue

                if key not in self.consumed_keys:
                    if key not in self.kpm_map:
                        self.kpm_map[key] = []

                    fields = []
                    for column_name in reader.fieldnames:
                        if row[column_name] == '':
                            continue
                        self.kpm_map[key].append(float(row[column_name]))
                        fields.append(column_name)

                    regex = re.search(r"\w*-(\d+)\.txt", file.name)
                    fields.append('file_id_number')
                    self.kpm_map[key].append(regex.group(1))

                    self.consumed_keys.add(key)
                    self._send_to_telegraf(
                        ue=ue, values=self.kpm_map[key], fields=fields, file_type=key[2]
                    )

        lock.release()

    def on_closed(self, event):
        super().on_closed(event)

    def _send_to_telegraf(self, ue: int, values: List, fields: List, file_type: int):
        """
        Envia métricas para o Telegraf via StatsD (sem tags).
        """
        pipe = self.statsd_client.pipeline()

        # timestamp convertido em nanos (se precisar usar em nome da métrica)
        timestamp = int(values[0] * 1e6)

        for i, field in enumerate(fields):
            if field == 'file_id_number':
                continue

            value = values[i]

            # converter pdcp_latency
            if field == 'DRB.PdcpSduDelayDl.UEID (pdcpLatency)':
                value = value * 0.1

            if 'UEID' not in field:
                stat = f"{field}_cell_{values[-1]}".replace(' ', '')
            else:
                stat = (field + '_' + ue).replace(' ', '')
                if file_type in (0, 3):
                    stat += '_up'
                elif file_type in (1, 4):
                    stat += '_cp'
                elif file_type == 2:
                    stat += '_du'

            # envia métrica
            pipe.gauge(stat, value)

        pipe.send()

        # debug no console
        print(f"[✓] Enviando métricas para o Telegraf:")
        for f, v in zip(fields, values):
            print(f"    {f}: {v}")


if __name__ == "__main__":
    event_handler = SimWatcher()
    observer = Observer()
    observer.schedule(event_handler, ".", recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
