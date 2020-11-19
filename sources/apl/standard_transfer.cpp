#include "apl/standard_transfer.h"

namespace apl
{

    standard_sender::standard_sender(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q): sender(), comm(_comm), proc(_proc), q(_q)
    { }

    void standard_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        try
        {
            ret = MPI_Send(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
    }

    MPI_Request standard_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        int ret = MPI_SUCCESS;
        MPI_Request req = MPI_REQUEST_NULL;
        try
        {
            ret = MPI_Isend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return req;
    }

    void standard_sender::store_request(MPI_Request req) const
    { q->push_back(req); }

    standard_receiver::standard_receiver(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q): receiver(), comm(_comm), proc(_proc), q(_q)
    { }

    MPI_Status standard_receiver::recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        int ret = MPI_SUCCESS;
        MPI_Status status;
        try
        {
            ret = MPI_Recv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &status);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return status;
    }

    MPI_Request standard_receiver::irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        int ret = MPI_SUCCESS;
        MPI_Request req = MPI_REQUEST_NULL;
        try
        {
            ret = MPI_Irecv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return req;
    }

    MPI_Status standard_receiver::probe_impl(TAG tg) const
    {
        int ret = MPI_SUCCESS;
        MPI_Status status;
        try
        {
            ret = MPI_Probe(proc, static_cast<int>(tg), comm, &status);
        }
        catch (...)
        {
            assert(ret == MPI_SUCCESS);
        }
        assert(ret == MPI_SUCCESS);
        return status;
    }

    void standard_receiver::store_request(MPI_Request req) const
    { q->push_back(req); }

}
