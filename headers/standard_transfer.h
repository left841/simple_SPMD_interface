#ifndef __STANDARD_TRANSFER_H__
#define __STANDARD_TRANSFER_H__

#include "transfer.h"

namespace auto_parallel
{

    class standard_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;
        std::queue<MPI_Request>* q;

    public:

        standard_sender(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q);

        virtual void send(const void* buf, int size, MPI_Datatype type) const;
        virtual void isend(const void* buf, int size, MPI_Datatype type) const;

    };

    class standard_receiver: public receiver
    {
    private:

        MPI_Comm comm;
        process proc;
        std::queue<MPI_Request>* q;

    public:

        standard_receiver(MPI_Comm _comm, process _proc, std::queue<MPI_Request>* _q);

        virtual void recv(void* buf, int size, MPI_Datatype type) const;
        virtual void irecv(void* buf, int size, MPI_Datatype type) const;
        virtual int probe(MPI_Datatype type) const;

    };

}

#endif // __STANDARD_TRANSFER_H__
