#include "apl/comm_group.h"

namespace apl
{

    comm_group::comm_group(): g(MPI_GROUP_NULL)
    { }

    comm_group::comm_group(const communicator& src)
    { apl_MPI_CHECKER(MPI_Comm_group(src.get_comm(), &g)); }

    comm_group::comm_group(const comm_group& src, process* ranks, size_t n, group_constructor_type type)
    {
        if (type == group_constructor_type::incl)
            apl_MPI_CHECKER(MPI_Group_incl(src.group(), static_cast<int>(n), ranks, &g));
        else
            apl_MPI_CHECKER(MPI_Group_excl(src.group(), static_cast<int>(n), ranks, &g));
    }

    comm_group::comm_group(const comm_group& src, int(*ranges)[3], size_t n, group_constructor_type type)
    {
        if (type == group_constructor_type::incl)
            apl_MPI_CHECKER(MPI_Group_range_incl(src.group(), static_cast<int>(n), ranges, &g));
        else
            apl_MPI_CHECKER(MPI_Group_range_excl(src.group(), static_cast<int>(n), ranges, &g));
    }

    comm_group::comm_group(comm_group&& src) noexcept: g(src.g)
    { src.g = MPI_GROUP_NULL; }

    comm_group& comm_group::operator=(comm_group&& src) noexcept
    {
        if (this == &src)
            return *this;
        this->~comm_group();
        g = src.g;
        src.g = MPI_GROUP_NULL;
        return *this;
    }

    comm_group::~comm_group()
    {
        if (g != MPI_GROUP_NULL)
            apl_MPI_CHECKER(MPI_Group_free(&g));
        g = MPI_GROUP_NULL;
    }

    int comm_group::size() const
    {
        int sz;
        apl_MPI_CHECKER(MPI_Group_size(g, &sz));
        return sz;
    }

    process comm_group::rank() const
    {
        int r;
        apl_MPI_CHECKER(MPI_Group_rank(g, &r));
        return r;
    }

    MPI_Group comm_group::group() const
    { return g; }

    bool comm_group::operator==(const comm_group& src) const
    {
        int cmp;
        apl_MPI_CHECKER(MPI_Group_compare(g, src.g, &cmp));
        if (cmp == MPI_IDENT)
            return true;
        return false;
    }

    bool comm_group::operator!=(const comm_group& src) const
    {
        int cmp;
        apl_MPI_CHECKER(MPI_Group_compare(g, src.g, &cmp));
        if (cmp != MPI_IDENT)
            return true;
        return false;
    }

    comm_group comm_group::operator&(const comm_group& src) const
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_intersection(g, src.g, &gr.g));
        return gr;
    }

    comm_group comm_group::operator|(const comm_group& src) const
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_union(g, src.g, &gr.g));
        return gr;
    }

    comm_group comm_group::operator/(const comm_group& src) const
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_difference(g, src.g, &gr.g));
        return gr;
    }

    comm_group& comm_group::operator&=(const comm_group& src)
    {
        if (this == &src)
            return *this;
        MPI_Group gr;
        apl_MPI_CHECKER(MPI_Group_intersection(g, src.g, &gr));
        this->~comm_group();
        g = gr;
        return *this;
    }

    comm_group& comm_group::operator|=(const comm_group& src)
    {
        if (this == &src)
            return *this;
        MPI_Group gr;
        apl_MPI_CHECKER(MPI_Group_union(g, src.g, &gr));
        this->~comm_group();
        g = gr;
        return *this;
    }

    comm_group& comm_group::operator/=(const comm_group& src)
    {
        if (this == &src)
            return *this;
        MPI_Group gr;
        apl_MPI_CHECKER(MPI_Group_difference(g, src.g, &gr));
        this->~comm_group();
        g = gr;
        return *this;
    }

    comm_group comm_group::incl(process* ranks, size_t n)
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_incl(g, static_cast<int>(n), ranks, &gr.g));
        return gr;
    }

    comm_group comm_group::excl(process* ranks, size_t n)
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_excl(g, static_cast<int>(n), ranks, &gr.g));
        return gr;
    }

    comm_group comm_group::range_incl(int (*ranges)[3], size_t n)
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_range_incl(g, static_cast<int>(n), ranges, &gr.g));
        return gr;
    }

    comm_group comm_group::range_excl(int (*ranges)[3], size_t n)
    {
        comm_group gr;
        apl_MPI_CHECKER(MPI_Group_range_excl(g, static_cast<int>(n), ranges, &gr.g));
        return gr;
    }

}