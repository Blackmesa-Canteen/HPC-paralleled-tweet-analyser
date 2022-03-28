import threading


class TestSendHandler(threading.Thread):
    def __init__(self, comm):
        threading.Thread.__init__(self)
        self.__comm = comm
        self.__total_processes = comm.Get_size()
        self.__rank = comm.Get_rank()

    def run(self):
        print("[debug] %s is sending" % self.__rank)
        received_data = self.__comm.gather(self.__rank, root=0)

        print("[debug] son's res?", received_data)