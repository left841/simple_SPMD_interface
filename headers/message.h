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

        virtual void send(const sender& se) = 0;
        virtual void recv(const receiver& re) = 0;

        void wait_requests();

        friend class intracomm;
    };

    class message: public sendable
    {
    public:

        message();
        virtual ~message();
    };

}

#endif // __MESSAGE_H__
