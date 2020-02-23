#include "intracomm.h"

namespace auto_parallel
{

    intracomm::intracomm(MPI_Comm _comm): communicator(_comm)
    { }

    intracomm::intracomm(const intracomm& c): communicator(c)
    { }

    intracomm::intracomm(const intracomm& c, int color, int key): communicator(c, color, key)
    { }

    intracomm::~intracomm()
    { }

    void intracomm::send(sendable* mes, int proc)
    {
        mes->wait_requests();
        sender se(comm, proc, &(mes->req_q));
        mes->send(se);
    }

    void intracomm::recv(sendable* mes, int proc)
    {
        mes->wait_requests();
        receiver re(comm, proc, &(mes->req_q));
        mes->recv(re);
    }

    void intracomm::bcast(sendable* mes, int proc)
    {
        int my_pos = (rank - proc + comm_size) % comm_size;
        int i = 1;
        if (my_pos > 0)
        {
            for (int sum = 1; sum < my_pos; i <<= 1, sum += i);
            recv(mes, (rank - i + comm_size) % comm_size);
        }
        for (; i < comm_size; i <<= 1)
        {
            if ((my_pos < i) && (my_pos + i < comm_size))
                send(mes, (rank + i) % comm_size);
        }
    }

    void intracomm::barrier()
    { MPI_Barrier(comm); }

    void intracomm::abort(int err)
    { MPI_Abort(comm, err); }

}
