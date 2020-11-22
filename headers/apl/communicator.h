#ifndef __COMMUNICATOR_H__
#define __COMMUNICATOR_H__

#include <utility>
#include "mpi.h"
#include "apl/parallel_defs.h"

namespace apl
{

    class communicator
    {
    protected:

        MPI_Comm comm;

        communicator();
        communicator(const communicator& c);
        communicator(communicator&& c) noexcept = default;
        communicator& operator=(const communicator& c);
        communicator& operator=(communicator&& c) noexcept = default;

    public:

        ~communicator();

        void assign(MPI_Comm _comm);
        void unassign();

        void dublicate(const communicator& c);
        void free();

        void abort(int err) const;

        process rank() const;
        MPI_Comm get_comm() const;
        int size() const;

    };

}

#endif // __COMMUNICATOR_H__
