#include "message.h"

namespace apl
{

    sendable::sendable()
    { }

    sendable::~sendable()
    { /*wait_requests();*/ }

    void sendable::wait_requests()
    {
        while (req_q.size())
        {
            MPI_Wait(&req_q.front(), MPI_STATUS_IGNORE);
            req_q.pop();
        }
    }

    template<>
    void sender::send<sendable>(const sendable* buf, int size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->send(*this);
    }

    template<>
    void sender::isend<sendable>(const sendable* buf, int size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->send(*this);
    }

    template<>
    void receiver::recv<sendable>(sendable* buf, int size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->recv(*this);
    }

    template<>
    void receiver::irecv<sendable>(sendable* buf, int size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->recv(*this);
    }

    message::message(): sendable()
    { }

    message::~message()
    { /*wait_requests();*/ }

}
