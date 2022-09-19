#include "apl/intracomm.h"
#include "apl/comm_group.h"

namespace apl
{

    intracomm::intracomm(): communicator()
    { }

    intracomm::intracomm(const intracomm& c): communicator(c)
    { }

    intracomm::intracomm(const intracomm& c, int color, int key)
    { split(c, color, key); }

    intracomm::intracomm(const intracomm& c, const comm_group& g)
    { apl_MPI_CHECKER(MPI_Comm_create(c.get_comm(), g.group(), &comm)); }

    intracomm::~intracomm()
    { }

    void intracomm::create(const intracomm& c, const comm_group& g)
    {
        if (comm != MPI_COMM_NULL)
            free();
        apl_MPI_CHECKER(MPI_Comm_create(c.get_comm(), g.group(), &comm));
    }

    void intracomm::split(const intracomm& c, int color, int key)
    { apl_MPI_CHECKER(MPI_Comm_split(c.comm, color, key, &comm)); }

    void intracomm::barrier() const
    { apl_MPI_CHECKER(MPI_Barrier(comm)); }

    process intracomm::wait_any_process() const
    {
        MPI_Status status;
        apl_MPI_CHECKER(MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status));
        return status.MPI_SOURCE;
    }

    process intracomm::test_any_process() const
    {
        int flag = 0;
        MPI_Status status;
        apl_MPI_CHECKER(MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &flag, &status));
        if (flag)
            return status.MPI_SOURCE;
        return MPI_PROC_NULL;
    }

    void intracomm::wait_process(process proc) const
    {
        MPI_Status status;
        apl_MPI_CHECKER(MPI_Probe(proc, MPI_ANY_TAG, comm, &status));
    }

    bool intracomm::test_process(process proc) const
    {
        int flag = 0;
        MPI_Status status;
        apl_MPI_CHECKER(MPI_Iprobe(proc, MPI_ANY_TAG, comm, &flag, &status));
        return flag;
    }

    global_intracomm::global_intracomm(): intracomm()
    { }

    global_intracomm::~global_intracomm()
    { comm = MPI_COMM_NULL; }

}
