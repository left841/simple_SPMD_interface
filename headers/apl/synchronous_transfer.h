#ifndef __SYNCHRONOUS_TRANSFER_H__
#define __SYNCHRONOUS_TRANSFER_H__

#include "apl/transfer.h"

namespace apl
{

    class synchronous_sender: public sender
    {
    private:

        MPI_Comm comm;
        process proc;

        void send_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;
        MPI_Request isend_impl(const void* buf, size_t size, const simple_datatype& type, TAG tg) const;

    public:

        synchronous_sender(MPI_Comm _comm, process _proc);

    };

}

#endif // __SYNCHRONOUS_TRANSFER_H__
