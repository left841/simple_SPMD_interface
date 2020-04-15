#ifndef __READY_TRANSFER_H__
#define __READY_TRANSFER_H__

#include "transfer.h"

namespace apl
{

    class ready_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;
        std::queue<MPI_Request>* q;

    public:

        ready_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q);

        virtual void send(const void* buf, int size, MPI_Datatype type) const;
        virtual void isend(const void* buf, int size, MPI_Datatype type) const;

        virtual void wait_all() const;

    };

}

#endif // __READY_TRANSFER_H__
