#ifndef __STANDARD_TRANSFER_H__
#define __STANDARD_TRANSFER_H__

#include "transfer.h"

namespace apl
{

    class standard_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;
        std::vector<MPI_Request>* q;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        standard_sender(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q);

        virtual void store_request(MPI_Request req) const;

    };

    class standard_receiver: public receiver
    {
    private:

        MPI_Comm comm;
        process proc;
        std::vector<MPI_Request>* q;

        MPI_Status recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Status probe_impl(TAG tg) const;

    public:

        standard_receiver(MPI_Comm _comm, process _proc, std::vector<MPI_Request>* _q);

        virtual void store_request(MPI_Request req) const;

    };

}

#endif // __STANDARD_TRANSFER_H__
