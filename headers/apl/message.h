#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <vector>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/request_container.h"
#include "apl/transfer.h"

namespace apl
{

    class message
    {
    public:

        message();
        virtual ~message();

        virtual void send(const sender& se) const = 0;
        virtual void recv(const receiver& re) = 0;
        virtual void isend(const sender& se, request_block& req) const;
        virtual void irecv(const receiver& re, request_block& req);
    };

    template<>
    void sender::send<message>(const message& buf) const;

    template<>
    void sender::isend<message>(const message& buf, request_block& req) const;

    template<>
    void receiver::recv<message>(message& buf) const;

    template<>
    void receiver::irecv<message>(message& buf, request_block& req) const;

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

        void send(const sender& se) const override final;
        void recv(const receiver& re) override final;
        void isend(const sender& se, request_block& req) const override final;
        void irecv(const receiver& re, request_block& req) override final;
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
    { se.send(*value); }

    template<typename Type>
    void message_wrapper<Type>::recv(const receiver& re)
    { re.recv(*value); }

    template<typename Type>
    void message_wrapper<Type>::isend(const sender& se, request_block& req) const
    { se.isend(*value, req); }

    template<typename Type>
    void message_wrapper<Type>::irecv(const receiver& re, request_block& req)
    { re.irecv(*value, req); }

}

#endif // __MESSAGE_H__
