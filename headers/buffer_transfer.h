#ifndef __BUFFER_TRANSFER_H__
#define __BUFFER_TRANSFER_H__

#include "transfer.h"

namespace auto_parallel
{

    class buffer_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;
        std::queue<MPI_Request>* q;

    public:

        buffer_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q);

        virtual void send(const void* buf, int size, MPI_Datatype type) const;
        virtual void isend(const void* buf, int size, MPI_Datatype type) const;

    };

}

#endif // __BUFFER_TRANSFER_H__
