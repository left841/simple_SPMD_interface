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

namespace apl
{

    class intracomm: public communicator
    {

    public:

        intracomm(MPI_Comm _comm = MPI_COMM_WORLD);
        explicit intracomm(const intracomm& c);
        intracomm(const intracomm& c, int color, int key);
        ~intracomm();

        void send(message* mes, process proc);
        void bsend(message* mes, process proc);
        void ssend(message* mes, process proc);
        void rsend(message* mes, process proc);
        void recv(message* mes, process proc);
        void bcast(message* mes, process proc);

        void barrier();
        void abort(int err);

    };

}

#endif // __INTRACOMM_H__
