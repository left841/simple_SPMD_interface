#ifndef __TRANSFER_H__
#define __TRANSFER_H__

#include <cstddef>
#include <cassert>
#include <climits>
#include <queue>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/request_container.h"

namespace apl
{

    template<typename Type, typename Member>
    constexpr size_t offset_of(Member Type::* member)
    { return reinterpret_cast<char*>(&(reinterpret_cast<Type*>(0)->*member)) - reinterpret_cast<char*>(0); }

    template<typename Type, typename Member, size_t off>
    struct type_offset
    {
        size_t offset;
        typedef Member type;

        type_offset(Member Type::* member)
        { offset = reinterpret_cast<char*>(&(reinterpret_cast<Type*>(0)->*member)) - reinterpret_cast<char*>(0); }
    };

    struct simple_datatype
    {
        ptrdiff_t size_in_bytes;
        MPI_Datatype type;

        simple_datatype(MPI_Datatype type);

        simple_datatype(std::vector<MPI_Datatype> types, std::vector<size_t> offsets);

        static std::vector<MPI_Datatype> created_datatypes;

        static void add_datatype(MPI_Datatype dt);
    };

    template<typename Type, ptrdiff_t offset = 0>
    const simple_datatype& datatype();

    //template<typename... Types, ptrdiff_t... offsets>
    //const simple_datatype& datatype()
    //{
    //    static simple_datatype d({datatype<Types>().type...}, {offsets...});
    //}

    const simple_datatype& byte_datatype();

    class sender
    {
    protected:

        virtual void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const = 0;
        virtual MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const = 0;

    public:

        sender();
        virtual ~sender();

        void send(const void* buf, size_t size, simple_datatype type) const;
        void isend(const void* buf, size_t size, simple_datatype type, request_block& req) const;

        void send_bytes(const void* buf, size_t count) const;
        void isend_bytes(const void* buf, size_t count, request_block& req) const;

        template<class T>
        void send(const T* buf, size_t size = 1) const;
        template<class T>
        void isend(const T* buf, size_t size, request_block& req) const;

    };

    template<class T>
    void sender::isend(const T* buf, size_t size, request_block& req) const
    { send(buf, size); }

    class receiver
    {
    protected:

        mutable bool probe_flag;

        virtual MPI_Status recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const = 0;
        virtual MPI_Request irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg = TAG::UNDEFINED) const = 0;
        virtual MPI_Status probe_impl(TAG tg = TAG::ANY) const = 0;

    public:

        receiver();
        virtual ~receiver();

        void recv(void* buf, size_t size, simple_datatype type) const;
        void irecv(void* buf, size_t size, simple_datatype type, request_block& req) const;
        size_t probe(simple_datatype type) const;

        void recv_bytes(void* buf, size_t count) const;
        void irecv_bytes(void* buf, size_t count, request_block& req) const;
        size_t probe_bytes() const;

        template<class T>
        void recv(T* buf, size_t size = 1) const;
        template<class T>
        void irecv(T* buf, size_t size, request_block& req) const;
        template<class T>
        size_t probe() const;

    };

    template<class T>
    void receiver::irecv(T* buf, size_t size, request_block& req) const
    { recv(buf, size); }

    //send-recv specifications
    // char
    template<>
    const simple_datatype& datatype<char>();

    template<>
    void sender::send<char>(const char* buf, size_t size) const;

    template<>
    void sender::isend<char>(const char* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<char>(char* buf, size_t size) const;

    template<>
    void receiver::irecv<char>(char* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<char>() const;

    // unsigned char
    template<>
    const simple_datatype& datatype<unsigned char>();

    template<>
    void sender::send<unsigned char>(const unsigned char* buf, size_t size) const;

    template<>
    void sender::isend<unsigned char>(const unsigned char* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<unsigned char>(unsigned char* buf, size_t size) const;

    template<>
    void receiver::irecv<unsigned char>(unsigned char* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<unsigned char>() const;

    // short
    template<>
    const simple_datatype& datatype<short>();

    template<>
    void sender::send<short>(const short* buf, size_t size) const;

    template<>
    void sender::isend<short>(const short* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<short>(short* buf, size_t size) const;

    template<>
    void receiver::irecv<short>(short* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<short>() const;

    // unsigned short
    template<>
    const simple_datatype& datatype<unsigned short>();

    template<>
    void sender::send<unsigned short>(const unsigned short* buf, size_t size) const;

    template<>
    void sender::isend<unsigned short>(const unsigned short* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<unsigned short>(unsigned short* buf, size_t size) const;

    template<>
    void receiver::irecv<unsigned short>(unsigned short* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<unsigned short>() const;

    // int
    template<>
    const simple_datatype& datatype<int>();

    template<>
    void sender::send<int>(const int* buf, size_t size) const;

    template<>
    void sender::isend<int>(const int* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<int>(int* buf, size_t size) const;

    template<>
    void receiver::irecv<int>(int* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<int>() const;

    // unsigned
    template<>
    const simple_datatype& datatype<unsigned>();

    template<>
    void sender::send<unsigned>(const unsigned* buf, size_t size) const;

    template<>
    void sender::isend<unsigned>(const unsigned* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<unsigned>(unsigned* buf, size_t size) const;

    template<>
    void receiver::irecv<unsigned>(unsigned* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<unsigned>() const;

    // long
    template<>
    const simple_datatype& datatype<long>();

    template<>
    void sender::send<long>(const long* buf, size_t size) const;

    template<>
    void sender::isend<long>(const long* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<long>(long* buf, size_t size) const;

    template<>
    void receiver::irecv<long>(long* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<long>() const;

    // unsigned long
    template<>
    const simple_datatype& datatype<unsigned long>();

    template<>
    void sender::send<unsigned long>(const unsigned long* buf, size_t size) const;

    template<>
    void sender::isend<unsigned long>(const unsigned long* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<unsigned long>(unsigned long* buf, size_t size) const;

    template<>
    void receiver::irecv<unsigned long>(unsigned long* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<unsigned long>() const;

    // long long
    template<>
    const simple_datatype& datatype<long long>();

    template<>
    void sender::send<long long>(const long long* buf, size_t size) const;

    template<>
    void sender::isend<long long>(const long long* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<long long>(long long* buf, size_t size) const;

    template<>
    void receiver::irecv<long long>(long long* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<long long>() const;

    // unsigned long long
    template<>
    const simple_datatype& datatype<unsigned long long>();

    template<>
    void sender::send<unsigned long long>(const unsigned long long* buf, size_t size) const;

    template<>
    void sender::isend<unsigned long long>(const unsigned long long* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<unsigned long long>(unsigned long long* buf, size_t size) const;

    template<>
    void receiver::irecv<unsigned long long>(unsigned long long* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<unsigned long long>() const;

    // float
    template<>
    const simple_datatype& datatype<float>();

    template<>
    void sender::send<float>(const float* buf, size_t size) const;

    template<>
    void sender::isend<float>(const float* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<float>(float* buf, size_t size) const;

    template<>
    void receiver::irecv<float>(float* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<float>() const;

    // double
    template<>
    const simple_datatype& datatype<double>();

    template<>
    void sender::send<double>(const double* buf, size_t size) const;

    template<>
    void sender::isend<double>(const double* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<double>(double* buf, size_t size) const;

    template<>
    void receiver::irecv<double>(double* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<double>() const;

    // long double
    template<>
    const simple_datatype& datatype<long double>();

    template<>
    void sender::send<long double>(const long double* buf, size_t size) const;

    template<>
    void sender::isend<long double>(const long double* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<long double>(long double* buf, size_t size) const;

    template<>
    void receiver::irecv<long double>(long double* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<long double>() const;

    // wchar_t
    template<>
    const simple_datatype& datatype<wchar_t>();

    template<>
    void sender::send<wchar_t>(const wchar_t* buf, size_t size) const;

    template<>
    void sender::isend<wchar_t>(const wchar_t* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<wchar_t>(wchar_t* buf, size_t size) const;

    template<>
    void receiver::irecv<wchar_t>(wchar_t* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<wchar_t>() const;

    // local_message_id
    template<>
    const simple_datatype& datatype<local_message_id>();

    template<>
    void sender::send<local_message_id>(const local_message_id* buf, size_t size) const;

    template<>
    void sender::isend<local_message_id>(const local_message_id* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<local_message_id>(local_message_id* buf, size_t size) const;

    template<>
    void receiver::irecv<local_message_id>(local_message_id* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<local_message_id>() const;

    // local_task_id
    template<>
    const simple_datatype& datatype<local_task_id>();

    template<>
    void sender::send<local_task_id>(const local_task_id* buf, size_t size) const;

    template<>
    void sender::isend<local_task_id>(const local_task_id* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<local_task_id>(local_task_id* buf, size_t size) const;

    template<>
    void receiver::irecv<local_task_id>(local_task_id* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<local_task_id>() const;

    // task_dependence
    template<>
    const simple_datatype& datatype<task_dependence>();

    template<>
    void sender::send<task_dependence>(const task_dependence* buf, size_t size) const;

    template<>
    void sender::isend<task_dependence>(const task_dependence* buf, size_t size, request_block& req) const;

    template<>
    void receiver::recv<task_dependence>(task_dependence* buf, size_t size) const;

    template<>
    void receiver::irecv<task_dependence>(task_dependence* buf, size_t size, request_block& req) const;

    template<>
    size_t receiver::probe<task_dependence>() const;

}

#endif // __TRANSFER_H__
