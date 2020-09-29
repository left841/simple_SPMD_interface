#include "message.h"

namespace apl
{

    message::message()
    { }

    message::~message()
    { /*wait_requests();*/ }

    void message::wait_requests()
    {
        while (req_q.size())
        {
            MPI_Wait(&req_q.front(), MPI_STATUS_IGNORE);
            req_q.pop();
        }
    }

    template<>
    void sender::send<message>(const message* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->send(*this);
    }

    template<>
    void sender::isend<message>(const message* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->send(*this);
    }

    template<>
    void receiver::recv<message>(message* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->recv(*this);
    }

    template<>
    void receiver::irecv<message>(message* buf, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            (buf + i)->recv(*this);
    }

}
