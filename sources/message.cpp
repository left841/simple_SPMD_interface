#include "message.h"

namespace apl
{

    message::message()
    { }

    message::~message()
    { }

    void message::wait_requests()
    {
        if (req_v.size())
        {
            MPI_Waitall(static_cast<int>(req_v.size()), req_v.data(), MPI_STATUSES_IGNORE);
            req_v.clear();
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
