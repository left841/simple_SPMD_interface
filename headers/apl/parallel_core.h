#ifndef __PARALLEL_CORE_H__
#define __PARALLEL_CORE_H__

#include <vector>
#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/transfer.h"
#include "apl/intracomm.h"

namespace apl
{

    class parallel_engine
    {
    private:

        static double start_time;
        static process global_comm_rank;

    public:

        parallel_engine(int* argc = nullptr, char*** argv = nullptr);
        virtual ~parallel_engine();

        void init_library(int* argc, char*** argv);
        void finalize_library();

        static double get_start_time();

        static process global_rank();
    };

    extern global_intracomm comm_world;
    extern global_intracomm comm_self;

}

#endif // __PARALLEL_CORE_H__
