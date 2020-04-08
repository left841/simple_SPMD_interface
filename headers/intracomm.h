#ifndef __INTRACOMM_H__
#define __INTRACOMM_H__

#include "mpi.h"
#include "parallel_defs.h"
#include "communicator.h"
#include "transfer.h"
#include "standard_transfer.h"
#include "buffer_transfer.h"
#include "synchronous_transfer.h"
#include "ready_transfer.h"
#include "message.h"

namespace auto_parallel
{

    class intracomm: public communicator
    {

    public:

        intracomm(MPI_Comm _comm = MPI_COMM_WORLD);
        explicit intracomm(const intracomm& c);
        intracomm(const intracomm& c, int color, int key);
        ~intracomm();

        void send(sendable* mes, process proc);
        void bsend(sendable* mes, process proc);
        void ssend(sendable* mes, process proc);
        void rsend(sendable* mes, process proc);
        void recv(sendable* mes, process proc);
        void bcast(sendable* mes, process proc);

        void barrier();
        void abort(int err);

    };

}

#endif // __INTRACOMM_H__
