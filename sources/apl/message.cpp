#include "apl/message.h"

namespace apl
{

    message::message()
    { }

    message::~message()
    { }

    void message::isend(const sender& se, request_block& req) const
    { send(se); }

    void message::irecv(const receiver& re, request_block& req)
    { recv(re); }

    template<>
    void sender::send<message>(const message& buf) const
    { buf.send(*this); }

    template<>
    void sender::isend<message>(const message& buf, request_block& req) const
    { buf.isend(*this, req); }

    template<>
    void receiver::recv<message>(message& buf) const
    { buf.recv(*this); }

    template<>
    void receiver::irecv<message>(message& buf, request_block& req) const
    { buf.irecv(*this, req); }

}
