#include "synchronous_transfer.h"

namespace apl
{

    synchronous_sender::synchronous_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void synchronous_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        try
        {
            ret = MPI_Ssend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
    }

    MPI_Request synchronous_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        MPI_Request req = MPI_REQUEST_NULL;
        try
        {
            ret = MPI_Issend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return req;
    }

    void synchronous_sender::wait_all() const
    {
        while (!q->empty())
        {
            MPI_Wait(&q->front(), MPI_STATUS_IGNORE);
            q->pop();
        }
    }

    void synchronous_sender::store_request(MPI_Request req) const
    { q->push(req); }

}
