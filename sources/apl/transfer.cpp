#include "apl/transfer.h"

namespace apl
{

    simple_datatype::simple_datatype(MPI_Datatype t): type(t)
    {
        MPI_Aint lb, extent;
        apl_MPI_CHECKER(MPI_Type_get_extent(t, &lb, &extent));
        size_in_bytes = extent - lb;
    }

    simple_datatype::simple_datatype(std::vector<MPI_Datatype> types, std::vector<size_t> offsets)
    {
        std::vector<int> block_legth(types.size());
        for (size_t i = 0; i < types.size(); ++i)
            block_legth[i] = 1;
        std::vector<MPI_Aint> mpi_offsets(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i)
            mpi_offsets[i] = offsets[i];
        apl_MPI_CHECKER(MPI_Type_create_struct(static_cast<int>(types.size()), block_legth.data(), mpi_offsets.data(), types.data(), &type));
        apl_MPI_CHECKER(MPI_Type_commit(&type));
        MPI_Aint lb, extent;
        apl_MPI_CHECKER(MPI_Type_get_extent(type, &lb, &extent));
        size_in_bytes = extent - lb;
        add_datatype(type);
    }

    simple_datatype::simple_datatype(const std::vector<MPI_Datatype>& types, const std::vector<size_t>& offsets, const std::vector<size_t>& block_lengths)
    {
        std::vector<int> block_legth(types.size());
        for (size_t i = 0; i < types.size(); ++i)
            block_legth[i] = block_lengths[i];
        std::vector<MPI_Aint> mpi_offsets(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i)
            mpi_offsets[i] = offsets[i];
        apl_MPI_CHECKER(MPI_Type_create_struct(static_cast<int>(types.size()), block_legth.data(), mpi_offsets.data(), const_cast<MPI_Datatype*>(types.data()), &type));
        apl_MPI_CHECKER(MPI_Type_commit(&type));
        MPI_Aint lb, extent;
        apl_MPI_CHECKER(MPI_Type_get_extent(type, &lb, &extent));
        size_in_bytes = extent - lb;
        add_datatype(type);
    }

    std::vector<MPI_Datatype> simple_datatype::created_datatypes;

    void simple_datatype::add_datatype(MPI_Datatype dt)
    { created_datatypes.push_back(dt); }

    const simple_datatype& byte_datatype()
    {
        static simple_datatype d(MPI_BYTE);
        return d;
    }

    sender::sender()
    { }

    sender::~sender()
    { }

    void sender::send(const void* buf, size_t size, simple_datatype type) const
    {
        if (size * type.size_in_bytes > INT_MAX)
        {
            send_impl(&size, 1, datatype<size_t>(), TAG::SIZE);
            size_t part = INT_MAX / type.size_in_bytes;
            while (size * type.size_in_bytes > INT_MAX)
            {
                send_impl(buf, part, type, TAG::MAIN);
                buf = reinterpret_cast<void*>(reinterpret_cast<size_t>(buf) + part * type.size_in_bytes);
                size -= part;
            }
        }
        send_impl(buf, size, type, TAG::MAIN);
    }

    void sender::isend(const void* buf, size_t size, simple_datatype type, request_block& req) const
    {
        if (size * type.size_in_bytes > INT_MAX)
        {
            send_impl(&size, 1, datatype<size_t>(), TAG::SIZE);
            size_t part = INT_MAX / type.size_in_bytes;
            while (size * type.size_in_bytes > INT_MAX)
            {
                req.store(isend_impl(buf, part, type, TAG::MAIN));
                buf = reinterpret_cast<void*>(reinterpret_cast<size_t>(buf) + part * type.size_in_bytes);
                size -= part;
            }
        }
        req.store(isend_impl(buf, size, type, TAG::MAIN));
    }

    void sender::send_bytes(const void* buf, size_t count) const
    { send(buf, count, byte_datatype()); }

    void sender::isend_bytes(const void* buf, size_t count, request_block& req) const
    { isend(buf, count, byte_datatype(), req); }

    receiver::receiver(): probe_flag(false)
    { }

    receiver::~receiver()
    { }

    void receiver::recv(void* buf, size_t size, simple_datatype type) const
    {
        if (size * type.size_in_bytes > INT_MAX)
        {
            if (probe_flag)
            {
                (void)recv_impl(&size, 1, datatype<size_t>(), TAG::SIZE);
                probe_flag = false;
            }
            size_t part = INT_MAX / type.size_in_bytes;
            while (size * sizeof(int) > INT_MAX)
            {
                (void)recv_impl(buf, part, type, TAG::MAIN);
                buf = reinterpret_cast<void*>(reinterpret_cast<size_t>(buf) + part * type.size_in_bytes);
                size -= part;
            }
        }
        (void)recv_impl(buf, size, type, TAG::MAIN);
    }

    void receiver::irecv(void* buf, size_t size, simple_datatype type, request_block& req) const
    {
        if (size * type.size_in_bytes > INT_MAX)
        {
            if (probe_flag)
            {
                (void)recv_impl(&size, 1, datatype<size_t>(), TAG::SIZE);
                probe_flag = false;
            }
            size_t part = INT_MAX / type.size_in_bytes;
            while (size * sizeof(int) > INT_MAX)
            {
                req.store(irecv_impl(buf, part, type, TAG::MAIN));
                buf = reinterpret_cast<void*>(reinterpret_cast<size_t>(buf) + part * type.size_in_bytes);
                size -= part;
            }
        }
        req.store(irecv_impl(buf, size, type, TAG::MAIN));
    }

    size_t receiver::probe(simple_datatype type) const
    {
        size_t size = 0;
        MPI_Status status = probe_impl();
        if (status.MPI_TAG == 0)
        {
            int sz = 0;
            apl_MPI_CHECKER(MPI_Get_count(&status, type.type, &sz));
            size = sz;
        }
        else
        {
            (void)recv_impl(&size, 1, datatype<size_t>(), TAG::SIZE);
            probe_flag = true;
        }
        return size;
    }

    void receiver::recv_bytes(void* buf, size_t count) const
    { recv(buf, count, byte_datatype()); }

    void receiver::irecv_bytes(void* buf, size_t count, request_block & req) const
    { irecv(buf, count, byte_datatype(), req); }

    size_t receiver::probe_bytes() const
    { return probe(byte_datatype()); }

    standard_sender::standard_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void standard_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Send(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request standard_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Isend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    standard_receiver::standard_receiver(MPI_Comm _comm, process _proc): receiver(), comm(_comm), proc(_proc)
    { }

    MPI_Status standard_receiver::recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        MPI_Status status {};
        apl_MPI_CHECKER(MPI_Recv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &status));
        return status;
    }

    MPI_Request standard_receiver::irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Irecv(buf, static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    MPI_Status standard_receiver::probe_impl(TAG tg) const
    {
        MPI_Status status {};
        apl_MPI_CHECKER(MPI_Probe(proc, static_cast<int>(tg), comm, &status));
        return status;
    }

    buffer_sender::buffer_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void buffer_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Bsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request buffer_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Ibsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    synchronous_sender::synchronous_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void synchronous_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Ssend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request synchronous_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Issend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

    ready_sender::ready_sender(MPI_Comm _comm, process _proc): sender(), comm(_comm), proc(_proc)
    { }

    void ready_sender::send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    { apl_MPI_CHECKER(MPI_Rsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm)); }

    MPI_Request ready_sender::isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const
    {
        MPI_Request req = MPI_REQUEST_NULL;
        apl_MPI_CHECKER(MPI_Irsend(const_cast<void*>(buf), static_cast<int>(size), type.type, proc, static_cast<int>(tg), comm, &req));
        return req;
    }

}
