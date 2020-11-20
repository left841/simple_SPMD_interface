#ifndef __INTRACOMM_H__
#define __INTRACOMM_H__

#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/communicator.h"
#include "apl/transfer.h"
#include "apl/standard_transfer.h"
#include "apl/buffer_transfer.h"
#include "apl/synchronous_transfer.h"
#include "apl/ready_transfer.h"
#include "apl/message.h"

namespace apl
{

    class intracomm: public communicator
    {

    public:

        intracomm(MPI_Comm _comm = MPI_COMM_WORLD);
        explicit intracomm(const intracomm& c);
        intracomm(const intracomm& c, int color, int key);
        ~intracomm();

        void send(message* mes, process proc, request_block& req);
        void bsend(message* mes, process proc, request_block& req);
        void ssend(message* mes, process proc, request_block& req);
        void rsend(message* mes, process proc, request_block& req);
        void recv(message* mes, process proc, request_block& req);
        void bcast(message* mes, process proc, request_block& req);

        void barrier();
        void abort(int err);

    };

}

#endif // __INTRACOMM_H__
