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
        std::vector<MPI_Request>* q;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        synchronous_sender(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q);

        virtual void store_request(MPI_Request req) const;

    };

}

#endif // __SYNCHRONOUS_TRANSFER_H__
