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

    //send-recv specifications
    // char
    template<>
    const simple_datatype& datatype<char>()
    {
        static simple_datatype d(MPI_SIGNED_CHAR);
        return d;
    }

    template<>
    void sender::send<char>(const char* buf, size_t size) const
    { send(buf, size, datatype<char>()); }

    template<>
    void sender::isend<char>(const char* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<char>(), req); }

    template<>
    void receiver::recv<char>(char* buf, size_t size) const
    { recv(buf, size, datatype<char>()); }

    template<>
    void receiver::irecv<char>(char* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<char>(), req); }

    template<>
    size_t receiver::probe<char>() const
    { return probe(datatype<char>()); }

    // unsigned char
    template<>
    const simple_datatype& datatype<unsigned char>()
    {
        static simple_datatype d(MPI_UNSIGNED_CHAR);
        return d;
    }

    template<>
    void sender::send<unsigned char>(const unsigned char* buf, size_t size) const
    { send(buf, size, datatype<unsigned char>()); }

    template<>
    void sender::isend<unsigned char>(const unsigned char* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<unsigned char>(), req); }

    template<>
    void receiver::recv<unsigned char>(unsigned char* buf, size_t size) const
    { recv(buf, size, datatype<unsigned char>()); }

    template<>
    void receiver::irecv<unsigned char>(unsigned char* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<unsigned char>(), req); }

    template<>
    size_t receiver::probe<unsigned char>() const
    { return probe(datatype<unsigned char>()); }

    // short
    template<>
    const simple_datatype& datatype<short>()
    {
        static simple_datatype d(MPI_SHORT);
        return d;
    }

    template<>
    void sender::send<short>(const short* buf, size_t size) const
    { send(buf, size, datatype<short>()); }

    template<>
    void sender::isend<short>(const short* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<short>(), req); }

    template<>
    void receiver::recv<short>(short* buf, size_t size) const
    { recv(buf, size, datatype<short>()); }

    template<>
    void receiver::irecv<short>(short* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<short>(), req); }

    template<>
    size_t receiver::probe<short>() const
    { return probe(datatype<short>()); }

    // unsigned short
    template<>
    const simple_datatype& datatype<unsigned short>()
    {
        static simple_datatype d(MPI_UNSIGNED_SHORT);
        return d;
    }

    template<>
    void sender::send<unsigned short>(const unsigned short* buf, size_t size) const
    { send(buf, size, datatype<unsigned short>()); }

    template<>
    void sender::isend<unsigned short>(const unsigned short* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<unsigned short>(), req); }

    template<>
    void receiver::recv<unsigned short>(unsigned short* buf, size_t size) const
    { recv(buf, size, datatype<unsigned short>()); }

    template<>
    void receiver::irecv<unsigned short>(unsigned short* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<unsigned short>(), req); }

    template<>
    size_t receiver::probe<unsigned short>() const
    { return probe(datatype<unsigned short>()); }

    // int
    template<>
    const simple_datatype& datatype<int>()
    {
        static simple_datatype d(MPI_INT);
        return d;
    }

    template<>
    void sender::send<int>(const int* buf, size_t size) const
    { send(buf, size, datatype<int>()); }

    template<>
    void sender::isend<int>(const int* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<int>(), req); }

    template<>
    void receiver::recv<int>(int* buf, size_t size) const
    { recv(buf, size, datatype<int>()); }

    template<>
    void receiver::irecv<int>(int* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<int>(), req); }

    template<>
    size_t receiver::probe<int>() const
    { return probe(datatype<int>()); }

    // unsigned
    template<>
    const simple_datatype& datatype<unsigned>()
    {
        static simple_datatype d(MPI_UNSIGNED);
        return d;
    }

    template<>
    void sender::send<unsigned>(const unsigned* buf, size_t size) const
    { send(buf, size, datatype<unsigned>()); }

    template<>
    void sender::isend<unsigned>(const unsigned* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<unsigned>(), req); }

    template<>
    void receiver::recv<unsigned>(unsigned* buf, size_t size) const
    { recv(buf, size, datatype<unsigned>()); }

    template<>
    void receiver::irecv<unsigned>(unsigned* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<unsigned>(), req); }

    template<>
    size_t receiver::probe<unsigned>() const
    { return probe(datatype<unsigned>()); }

    // long
    template<>
    const simple_datatype& datatype<long>()
    {
        static simple_datatype d(MPI_LONG);
        return d;
    }

    template<>
    void sender::send<long>(const long* buf, size_t size) const
    { send(buf, size, datatype<long>()); }

    template<>
    void sender::isend<long>(const long* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<long>(), req); }

    template<>
    void receiver::recv<long>(long* buf, size_t size) const
    { recv(buf, size, datatype<long>()); }

    template<>
    void receiver::irecv<long>(long* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<long>(), req); }

    template<>
    size_t receiver::probe<long>() const
    { return probe(datatype<long>()); }

    // unsigned long
    template<>
    const simple_datatype& datatype<unsigned long>()
    {
        static simple_datatype d(MPI_UNSIGNED_LONG);
        return d;
    }

    template<>
    void sender::send<unsigned long>(const unsigned long* buf, size_t size) const
    { send(buf, size, datatype<unsigned long>()); }

    template<>
    void sender::isend<unsigned long>(const unsigned long* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<unsigned long>(), req); }

    template<>
    void receiver::recv<unsigned long>(unsigned long* buf, size_t size) const
    { recv(buf, size, datatype<unsigned long>()); }

    template<>
    void receiver::irecv<unsigned long>(unsigned long* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<unsigned long>(), req); }

    template<>
    size_t receiver::probe<unsigned long>() const
    { return probe(datatype<unsigned long>()); }

    // long long
    template<>
    const simple_datatype& datatype<long long>()
    {
        static simple_datatype d(MPI_LONG_LONG);
        return d;
    }

    template<>
    void sender::send<long long>(const long long* buf, size_t size) const
    { send(buf, size, datatype<long long>()); }

    template<>
    void sender::isend<long long>(const long long* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<long long>(), req); }

    template<>
    void receiver::recv<long long>(long long* buf, size_t size) const
    { recv(buf, size, datatype<long long>()); }

    template<>
    void receiver::irecv<long long>(long long* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<long long>(), req); }

    template<>
    size_t receiver::probe<long long>() const
    { return probe(datatype<long long>()); }

    // unsigned long long
    template<>
    const simple_datatype& datatype<unsigned long long>()
    {
        static simple_datatype d(MPI_UNSIGNED_LONG_LONG);
        return d;
    }

    template<>
    void sender::send<unsigned long long>(const unsigned long long* buf, size_t size) const
    { send(buf, size, datatype<unsigned long long>()); }

    template<>
    void sender::isend<unsigned long long>(const unsigned long long* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<unsigned long long>(), req); }

    template<>
    void receiver::recv<unsigned long long>(unsigned long long* buf, size_t size) const
    { recv(buf, size, datatype<unsigned long long>()); }

    template<>
    void receiver::irecv<unsigned long long>(unsigned long long* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<unsigned long long>(), req); }

    template<>
    size_t receiver::probe<unsigned long long>() const
    { return probe(datatype<unsigned long long>()); }

    // float
    template<>
    const simple_datatype& datatype<float>()
    {
        static simple_datatype d(MPI_FLOAT);
        return d;
    }

    template<>
    void sender::send<float>(const float* buf, size_t size) const
    { send(buf, size, datatype<float>()); }

    template<>
    void sender::isend<float>(const float* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<float>(), req); }

    template<>
    void receiver::recv<float>(float* buf, size_t size) const
    { recv(buf, size, datatype<float>()); }

    template<>
    void receiver::irecv<float>(float* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<float>(), req); }

    template<>
    size_t receiver::probe<float>() const
    { return probe(datatype<float>()); }

    // double
    template<>
    const simple_datatype& datatype<double>()
    {
        static simple_datatype d(MPI_DOUBLE);
        return d;
    }

    template<>
    void sender::send<double>(const double* buf, size_t size) const
    { send(buf, size, datatype<double>()); }

    template<>
    void sender::isend<double>(const double* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<double>(), req); }

    template<>
    void receiver::recv<double>(double* buf, size_t size) const
    { recv(buf, size, datatype<double>()); }

    template<>
    void receiver::irecv<double>(double* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<double>(), req); }

    template<>
    size_t receiver::probe<double>() const
    { return probe(datatype<double>()); }

    // long double
    template<>
    const simple_datatype& datatype<long double>()
    {
        static simple_datatype d(MPI_LONG_DOUBLE);
        return d;
    }

    template<>
    void sender::send<long double>(const long double* buf, size_t size) const
    { send(buf, size, datatype<long double>()); }

    template<>
    void sender::isend<long double>(const long double* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<long double>(), req); }

    template<>
    void receiver::recv<long double>(long double* buf, size_t size) const
    { recv(buf, size, datatype<long double>()); }

    template<>
    void receiver::irecv<long double>(long double* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<long double>(), req); }

    template<>
    size_t receiver::probe<long double>() const
    { return probe(datatype<long double>()); }

    // wchar_t
    template<>
    const simple_datatype& datatype<wchar_t>()
    {
        static simple_datatype d(MPI_WCHAR);
        return d;
    }

    template<>
    void sender::send<wchar_t>(const wchar_t* buf, size_t size) const
    { send(buf, size, datatype<wchar_t>()); }

    template<>
    void sender::isend<wchar_t>(const wchar_t* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<wchar_t>(), req); }

    template<>
    void receiver::recv<wchar_t>(wchar_t* buf, size_t size) const
    { recv(buf, size, datatype<wchar_t>()); }

    template<>
    void receiver::irecv<wchar_t>(wchar_t* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<wchar_t>(), req); }

    template<>
    size_t receiver::probe<wchar_t>() const
    { return probe(datatype<wchar_t>()); }

}
