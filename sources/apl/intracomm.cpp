#include "apl/intracomm.h"

namespace apl
{

    intracomm::intracomm(MPI_Comm _comm): communicator(_comm)
    { }

    intracomm::intracomm(const intracomm& c): communicator(c)
    { }

    intracomm::intracomm(const intracomm& c, int color, int key): communicator(c, color, key)
    { }

    intracomm::~intracomm()
    { }

    void intracomm::send(message* mes, process proc, request_block& req)
    {
        standard_sender se(comm, proc, req);
        mes->send(se);
    }

    void intracomm::bsend(message* mes, process proc, request_block& req)
    {
        buffer_sender se(comm, proc, req);
        mes->send(se);
    }

    void intracomm::ssend(message* mes, process proc, request_block& req)
    {
        synchronous_sender se(comm, proc, req);
        mes->send(se);
    }

    void intracomm::rsend(message* mes, process proc, request_block& req)
    {
        ready_sender se(comm, proc, req);
        mes->send(se);
    }

    void intracomm::recv(message* mes, process proc, request_block& req)
    {
        standard_receiver re(comm, proc, req);
        mes->recv(re);
    }

    void intracomm::bcast(message* mes, process proc, request_block& req)
    {
        int my_pos = (comm_rank - proc + comm_size) % comm_size;
        int i = 1;
        if (my_pos > 0)
        {
            for (int sum = 1; sum < my_pos; i <<= 1, sum += i);
            recv(mes, (comm_rank - i + comm_size) % comm_size, req);
        }
        for (; i < comm_size; i <<= 1)
        {
            if ((my_pos < i) && (my_pos + i < comm_size))
            {
                req.wait_all();
                send(mes, (comm_rank + i) % comm_size, req);
            }
        }
    }

    void intracomm::barrier()
    { apl_MPI_CHECKER(MPI_Barrier(comm)); }

    void intracomm::abort(int err)
    { apl_MPI_CHECKER(MPI_Abort(comm, err)); }

}
