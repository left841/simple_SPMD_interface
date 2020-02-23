#ifndef __TRANSFER_H__
#define __TRANSFER_H__

#include <queue>
#include <climits>
#include "mpi.h"

namespace auto_parallel
{

    class sender
    {
    private:

        MPI_Comm comm;
        int proc;
        mutable int tag;
        std::queue<MPI_Request>* q;

    public:

        sender(MPI_Comm _comm, int _proc, std::queue<MPI_Request>* _q);

        void send(void* buf, int size, MPI_Datatype type) const;
        void isend(void* buf, int size, MPI_Datatype type) const;
        void big_send(void* buf, size_t size, MPI_Datatype type) const;
        void big_isend(void* buf, size_t size, MPI_Datatype type) const;
        template<class T>
        void send(T* buf, int size = 1);
        template<class T>
        void isend(T* buf, int size = 1);

    };

    template<class T>
    void sender::send(T* buf, int size)
    { MPI_Send(buf, size * sizeof(T), MPI_BYTE, proc, tag++, comm); }

    template<class T>
    void sender::isend(T* buf, int size)
    {
        MPI_Request req;
        MPI_Isend(buf, size * sizeof(T), MPI_BYTE, proc, tag++, comm, &req);
        q->push(req);
    }

    class receiver
    {
    private:

        MPI_Comm comm;
        int proc;
        mutable int tag;
        std::queue<MPI_Request>* q;

    public:

        receiver(MPI_Comm _comm, int _proc, std::queue<MPI_Request>* _q);

        void recv(void* buf, int size, MPI_Datatype type) const;
        void irecv(void* buf, int size, MPI_Datatype type) const;
        int probe(MPI_Datatype type) const;
        void big_recv(void* buf, size_t size, MPI_Datatype type) const;
        void big_irecv(void* buf, size_t size, MPI_Datatype type) const;
        template<class T>
        void recv(T* buf, int size = 1);
        template<class T>
        void irecv(T* buf, int size = 1);

    };

    template<class T>
    void receiver::recv(T* buf, int size)
    { MPI_Recv(buf, size * sizeof(T), MPI_BYTE, proc, tag++, comm, MPI_STATUS_IGNORE); }

    template<class T>
    void receiver::irecv(T* buf, int size)
    {
        MPI_Request req;
        MPI_Irecv(buf, size * sizeof(T), MPI_BYTE, proc, tag++, comm, &req);
        q->push(req);
    }


}

#endif // __TRANSFER_H__
