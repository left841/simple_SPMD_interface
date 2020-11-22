#ifndef __STANDARD_TRANSFER_H__
#define __STANDARD_TRANSFER_H__

#include "apl/transfer.h"

namespace apl
{

    class standard_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        standard_sender(MPI_Comm _comm, process _proc);

    };

    class standard_receiver: public receiver
    {
    private:

        MPI_Comm comm;
        process proc;

        MPI_Status recv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request irecv_impl(void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Status probe_impl(TAG tg) const;

    public:

        standard_receiver(MPI_Comm _comm, process _proc);

    };

}

#endif // __STANDARD_TRANSFER_H__
