# author: Xiaotian Li
# desc: thread for gathering all info from all processes in the mater node
import threading
import time

from mpi4py import MPI


class ResultGatherHandler(threading.Thread):
    def __init__(self, comm):
        threading.Thread.__init__(self)
        self.__comm = comm
        self.__total_processes = comm.Get_size()
        self.__rank = comm.Get_rank()
        self.__received_data = []

    def run(self):
        print("[debug] process %s start listening all results" % self.__rank)
        print("[debug] listening...")
        self.__received_data = self.__comm.gather(None, root=self.__rank)
        print("[debug] done...")

    def get_received_data(self):
        return self.__received_data

    def is_finished_receiving(self):
        if len(self.__received_data) != self.__total_processes:
            return False
        return True
