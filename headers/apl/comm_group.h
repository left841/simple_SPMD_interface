#ifndef __COMM_GROUP_H__
#define __COMM_GROUP_H__

#include <utility>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/communicator.h"

namespace apl
{
    
    enum class group_constructor_type
    {
        incl, excl
    };

    class comm_group
    {
    private:

        MPI_Group g;

    public:
        
        comm_group();
        comm_group(const communicator& src);
        comm_group(const comm_group& src, process* ranks, size_t n, group_constructor_type type);
        comm_group(const comm_group& src, int (*ranges)[3], size_t n, group_constructor_type type);
        comm_group(comm_group&& src) noexcept;
        comm_group& operator=(comm_group&& src) noexcept;
        ~comm_group();

        int size() const;
        process rank() const;
        MPI_Group group() const;

        bool operator==(const comm_group& src) const;
        bool operator!=(const comm_group& src) const;

        comm_group operator&(const comm_group& src) const;
        comm_group operator|(const comm_group& src) const;
        comm_group operator/(const comm_group& src) const;

        comm_group& operator&=(const comm_group& src);
        comm_group& operator|=(const comm_group& src);
        comm_group& operator/=(const comm_group& src);

        comm_group incl(process* ranks, size_t n);
        comm_group excl(process* ranks, size_t n);
        comm_group range_incl(int (*ranges)[3], size_t n);
        comm_group range_excl(int (*ranges)[3], size_t n);

    };

}

#endif // __COMM_GROUP_H__