#ifndef __SYNCHRONOUS_TRANSFER_H__
#define __SYNCHRONOUS_TRANSFER_H__

#include "transfer.h"

namespace apl
{

    class synchronous_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;
        std::queue<MPI_Request>* q;

    public:

        synchronous_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q);

        virtual void send(const void* buf, int size, MPI_Datatype type) const;
        virtual void isend(const void* buf, int size, MPI_Datatype type) const;

    };

}

#endif // __SYNCHRONOUS_TRANSFER_H__
