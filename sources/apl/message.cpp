#include "apl/message.h"

namespace apl
{

    message::message()
    { }

    message::~message()
    { }

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
