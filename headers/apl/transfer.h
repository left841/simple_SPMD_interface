#ifndef __TRANSFER_H__
#define __TRANSFER_H__

#include <cstddef>
#include <cassert>
#include <climits>
#include <queue>
#include <type_traits>
#include "mpi.h"
#include "apl/request_container.h"

namespace apl
{

    struct simple_datatype
    {
        ptrdiff_t size_in_bytes;
        MPI_Datatype type;

        simple_datatype(MPI_Datatype type);
        simple_datatype(std::vector<MPI_Datatype> types, std::vector<size_t> offsets);
        simple_datatype(const std::vector<MPI_Datatype>& types, const std::vector<size_t>& offsets, const std::vector<size_t>& block_lengths);
        simple_datatype(simple_datatype s_type, MPI_Aint lb, MPI_Aint extent);

        static std::vector<MPI_Datatype> created_datatypes;

        static void add_datatype(MPI_Datatype dt);
    };

    template<class Type>
    struct simple_datatype_map
    { using map = void; };

    template<class Type, MPI_Aint lb, MPI_Aint extent>
    struct datatype_map
    { using map = void; };

    template<typename... MapParts>
    class type_map
    {
    public:
        static const simple_datatype& get()
        {
            static simple_datatype d({MapParts::type()...}, {MapParts::offset()...}, {MapParts::block_length()...});
            return d;
        }
    };

    template<typename SubMap, MPI_Aint lb, MPI_Aint extent>
    class resized_type_map
    {
    public:
        static const simple_datatype& get()
        {
            static simple_datatype d(SubMap::get(), lb, extent);
            return d;
        }
    };

    template<typename Type>
    class is_simple_datatype
    {
    public:
        static constexpr bool value = !std::is_void<typename simple_datatype_map<Type>::map>::value;
    };

    template<typename Type>
    std::enable_if_t<!std::is_enum<Type>::value, const simple_datatype&> datatype()
    { return resized_type_map<typename simple_datatype_map<Type>::map, 0, sizeof(Type)>::get(); }

    template<typename Type>
    std::enable_if_t<std::is_enum<Type>::value, const simple_datatype&> datatype()
    { return resized_type_map<typename simple_datatype_map<std::underlying_type_t<Type>>::map, 0, sizeof(Type)>::get(); }

    template<typename Type, size_t Offset, size_t BlockLength = 1>
    struct type_offset
    {
        static MPI_Datatype type()
        { return datatype<Type>().type; }
        static constexpr size_t offset()
        { return Offset; }
        static constexpr size_t block_length()
        { return BlockLength; }
    };

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
        std::enable_if_t<is_simple_datatype<T>::value> send(const T& val) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> send(const T& val) const;
        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> isend(const T& val, request_block& req) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> isend(const T& val, request_block& req) const;

        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> send(const T* buf, size_t size) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> send(const T* buf, size_t size) const;
        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> isend(const T* buf, size_t size, request_block& req) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> isend(const T* buf, size_t size, request_block& req) const;

    };

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> sender::send(const T& val) const
    { send(&val, 1, datatype<T>()); }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> sender::isend(const T& val, request_block& req) const
    { isend(&val, 1, datatype<T>(), req); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> sender::isend(const T& val, request_block& req) const
    { send(val); }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> sender::send(const T* buf, size_t size) const
    { send(buf, size, datatype<T>()); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> sender::send(const T* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            send(buf[i]);
    }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> sender::isend(const T* buf, size_t size, request_block& req) const
    { isend(buf, size, datatype<T>(), req); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> sender::isend(const T* buf, size_t size, request_block& req) const
    {
        for (size_t i = 0; i < size; ++i)
            isend(buf[i], req);
    }

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
        std::enable_if_t<is_simple_datatype<T>::value> recv(T& val) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> recv(T& val) const;
        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> irecv(T& val, request_block& req) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> irecv(T& val, request_block& req) const;

        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> recv(T* buf, size_t size) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> recv(T* buf, size_t size) const;
        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value> irecv(T* buf, size_t size, request_block& req) const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value> irecv(T* buf, size_t size, request_block& req) const;
        template<class T>
        std::enable_if_t<is_simple_datatype<T>::value, size_t> probe() const;
        template<class T>
        std::enable_if_t<!is_simple_datatype<T>::value, size_t> probe() const;

    };

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> receiver::recv(T& val) const
    { recv(&val, 1, datatype<T>()); }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> receiver::irecv(T& val, request_block& req) const
    { irecv(&val, 1, datatype<T>(), req); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> receiver::irecv(T& val, request_block& req) const
    { recv(val); }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> receiver::recv(T* buf, size_t size) const
    { recv(buf, size, datatype<T>()); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> receiver::recv(T* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            recv(buf[i]);
    }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value> receiver::irecv(T* buf, size_t size, request_block& req) const
    { irecv(buf, size, datatype<T>(), req); }

    template<class T>
    std::enable_if_t<!is_simple_datatype<T>::value> receiver::irecv(T* buf, size_t size, request_block& req) const
    {
        for (size_t i = 0; i < size; ++i)
            irecv(buf[i], req);
    }

    template<class T>
    std::enable_if_t<is_simple_datatype<T>::value, size_t> receiver::probe() const
    { return probe(datatype<T>()); }

    class standard_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        standard_sender(MPI_Comm _comm, process _proc);

    };

    class standard_receiver: public receiver
    {
    private:

        MPI_Comm comm;
        process proc;

        MPI_Status recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Status probe_impl(TAG tg) const;

    public:

        standard_receiver(MPI_Comm _comm, process _proc);

    };

    class buffer_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        buffer_sender(MPI_Comm _comm, process _proc);

    };

    class synchronous_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        synchronous_sender(MPI_Comm _comm, process _proc);

    };

    class ready_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        ready_sender(MPI_Comm _comm, process _proc);

    };

    struct byte_map
    {
        static const simple_datatype& get()
        {
            static simple_datatype d(MPI_BYTE);
            return d;
        }
    };

    //send-recv specifications
    // char
    template<>
    struct simple_datatype_map<char>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_SIGNED_CHAR);
                return d;
            }
        };
    };

    // unsigned char
    template<>
    struct simple_datatype_map<unsigned char>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_UNSIGNED_CHAR);
                return d;
            }
        };
    };

    // short
    template<>
    struct simple_datatype_map<short>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_SHORT);
                return d;
            }
        };
    };

    // unsigned short
    template<>
    struct simple_datatype_map<unsigned short>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_UNSIGNED_SHORT);
                return d;
            }
        };
    };

    // int
    template<>
    struct simple_datatype_map<int>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_INT);
                return d;
            }
        };
    };

    // unsigned
    template<>
    struct simple_datatype_map<unsigned>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_UNSIGNED);
                return d;
            }
        };
    };

    // long
    template<>
    struct simple_datatype_map<long>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_LONG);
                return d;
            }
        };
    };

    // unsigned long
    template<>
    struct simple_datatype_map<unsigned long>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_UNSIGNED_LONG);
                return d;
            }
        };
    };

    // long long
    template<>
    struct simple_datatype_map<long long>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_LONG_LONG);
                return d;
            }
        };
    };

    // unsigned long long
    template<>
    struct simple_datatype_map<unsigned long long>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_UNSIGNED_LONG_LONG);
                return d;
            }
        };
    };

    // float
    template<>
    struct simple_datatype_map<float>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_FLOAT);
                return d;
            }
        };
    };

    // double
    template<>
    struct simple_datatype_map<double>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_DOUBLE);
                return d;
            }
        };
    };

    // long double
    template<>
    struct simple_datatype_map<long double>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_LONG_DOUBLE);
                return d;
            }
        };
    };

    // wchar_t
    template<>
    struct simple_datatype_map<wchar_t>
    {
        struct map
        {
            static const simple_datatype& get()
            {
                static simple_datatype d(MPI_WCHAR);
                return d;
            }
        };
    };

    template<typename Type, size_t Size>
    struct simple_datatype_map<Type[Size]>
    {
        using map = type_map<type_offset<Type, 0, Size>>;
    };

}

#endif // __TRANSFER_H__
