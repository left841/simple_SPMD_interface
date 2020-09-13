#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <queue>
#include <vector>
#include "mpi.h"
#include "parallel_defs.h"
#include "transfer.h"

namespace apl
{

    class sendable
    {
    private:

        std::queue<MPI_Request> req_q;

    public:

        sendable();
        virtual ~sendable();

        virtual void send(const sender& se) const = 0;
        virtual void recv(const receiver& re) = 0;

        void wait_requests();

        friend class intracomm;
    };

    template<>
    void sender::send<sendable>(const sendable* buf, int size) const;

    template<>
    void sender::isend<sendable>(const sendable* buf, int size) const;

    template<>
    void receiver::recv<sendable>(sendable* buf, int size) const;

    template<>
    void receiver::irecv<sendable>(sendable* buf, int size) const;

    class message: public sendable
    {
    public:

        message();
        virtual ~message();
    };

    template<typename Type>
    class message_wrapper: public message
    {
    private:

        Type* value;

    public:

        message_wrapper(Type* src);
        ~message_wrapper();
        
        operator Type&();
        operator const Type&() const;

        Type* get();

        void send(const sender& se) const;
        void recv(const receiver& re);
    };

    template<typename Type>
    message_wrapper<Type>::message_wrapper(Type* src): message(), value(src)
    { }

    template<typename Type>
    message_wrapper<Type>::~message_wrapper()
    { delete value; }

    template<typename Type>
    message_wrapper<Type>::operator Type&()
    { return *value; }

    template<typename Type>
    message_wrapper<Type>::operator const Type&() const
    { return *value; }

    template<typename Type>
    Type* message_wrapper<Type>::get()
    { return value; }

    template<typename Type>
    void message_wrapper<Type>::send(const sender& se) const
    { se.isend(value); }

    template<typename Type>
    void message_wrapper<Type>::recv(const receiver& re)
    { re.irecv(value); }

}

#endif // __MESSAGE_H__
