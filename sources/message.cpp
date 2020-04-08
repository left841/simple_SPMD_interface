#include "message.h"

namespace apl
{

    sendable::sendable()
    { }

    sendable::~sendable()
    { }

    void sendable::wait_requests()
    {
        while (req_q.size())
        {
            MPI_Wait(&req_q.front(), MPI_STATUS_IGNORE);
            req_q.pop();
        }
    }

    message::message(): sendable()
    { }

    message::~message()
    { }

}
