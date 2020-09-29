#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <queue>
#include <vector>
#include "mpi.h"
#include "parallel_defs.h"
#include "transfer.h"

namespace apl
{

    class message
    {
    private:

        std::queue<MPI_Request> req_q;

    public:

        message();
        virtual ~message();

        virtual void send(const sender& se) const = 0;
        virtual void recv(const receiver& re) = 0;

        void wait_requests();

        friend class intracomm;
    };

    template<>
    void sender::send<message>(const message* buf, size_t size) const;

    template<>
    void sender::isend<message>(const message* buf, size_t size) const;

    template<>
    void receiver::recv<message>(message* buf, size_t size) const;

    template<>
    void receiver::irecv<message>(message* buf, size_t size) const;

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
        const Type* get() const;

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
    const Type* message_wrapper<Type>::get() const
    { return value; }

    template<typename Type>
    void message_wrapper<Type>::send(const sender& se) const
    { se.send(value); }

    template<typename Type>
    void message_wrapper<Type>::recv(const receiver& re)
    { re.recv(value); }

}

#endif // __MESSAGE_H__
