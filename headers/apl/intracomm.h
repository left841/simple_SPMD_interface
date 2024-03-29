#ifndef __INTRACOMM_H__
#define __INTRACOMM_H__

#include "mpi.h"
#include "apl/parallel_defs.h"
#include "apl/communicator.h"
#include "apl/transfer.h"

namespace apl
{

    class comm_group;

    class intracomm: public communicator
    {
    public:

        intracomm();
        intracomm(const intracomm& c);
        intracomm(const intracomm& c, int color, int key);
        intracomm(const intracomm& c, const comm_group& g);
        intracomm(intracomm&& c) noexcept = default;
        intracomm& operator=(const intracomm& c) = default;
        intracomm& operator=(intracomm&& c) noexcept = default;
        ~intracomm();

        void create(const intracomm& c, const comm_group& g);
        void split(const intracomm& c, int color, int key);

        template<typename Type>
        void send(const Type* ptr, process proc) const;
        template<typename Type>
        void isend(const Type* ptr, process proc, request_block& req) const;
        template<typename Type>
        void cisend(const Type* ptr, process proc, request_block& req, size_t condition_size) const;
        template<typename Type>
        void bsend(const Type* ptr, process proc) const;
        template<typename Type>
        void ibsend(const Type* ptr, process proc, request_block& req) const;
        template<typename Type>
        void ssend(const Type* ptr, process proc) const;
        template<typename Type>
        void issend(const Type* ptr, process proc, request_block& req) const;
        template<typename Type>
        void rsend(const Type* ptr, process proc) const;
        template<typename Type>
        void irsend(const Type* ptr, process proc, request_block& req) const;
        template<typename Type>
        void recv(Type* ptr, process proc) const;
        template<typename Type>
        void irecv(Type* ptr, process proc, request_block& req) const;
        template<typename Type>
        void cirecv(Type* ptr, process proc, request_block& req, size_t condition_size) const;
        template<typename Type>
        void bcast(Type* ptr, process root) const;
        template<typename Type>
        void ibcast(Type* ptr, process root, request_block& req) const;

        void barrier() const;

        process wait_any_process() const;
        process test_any_process() const;
        void wait_process(process proc) const;
        bool test_process(process proc) const;

    };

    template<typename Type>
    void intracomm::send(const Type* ptr, process proc) const
    {
        standard_sender se(comm, proc);
        se.send(*ptr);
    }

    template<typename Type>
    void intracomm::isend(const Type* ptr, process proc, request_block& req) const
    {
        standard_sender se(comm, proc);
        se.isend(*ptr, req);
    }

    template<typename Type>
    inline void intracomm::cisend(const Type* ptr, process proc, request_block& req, size_t condition_size) const
    {
        condition_sender se(comm, proc, condition_size);
        se.isend(*ptr, req);
    }

    template<typename Type>
    void intracomm::bsend(const Type* ptr, process proc) const
    {
        buffer_sender se(comm, proc);
        se.send(*ptr);
    }

    template<typename Type>
    void intracomm::ibsend(const Type* ptr, process proc, request_block& req) const
    {
        buffer_sender se(comm, proc);
        se.isend(*ptr, req);
    }

    template<typename Type>
    void intracomm::ssend(const Type* ptr, process proc) const
    {
        synchronous_sender se(comm, proc);
        se.send(*ptr);
    }

    template<typename Type>
    void intracomm::issend(const Type* ptr, process proc, request_block& req) const
    {
        synchronous_sender se(comm, proc);
        se.isend(*ptr, req);
    }

    template<typename Type>
    void intracomm::rsend(const Type* ptr, process proc) const
    {
        ready_sender se(comm, proc);
        se.send(*ptr);
    }

    template<typename Type>
    void intracomm::irsend(const Type* ptr, process proc, request_block& req) const
    {
        ready_sender se(comm, proc);
        se.isend(*ptr, req);
    }

    template<typename Type>
    void intracomm::recv(Type* ptr, process proc) const
    {
        standard_receiver re(comm, proc);
        re.recv(*ptr);
    }

    template<typename Type>
    void intracomm::irecv(Type* ptr, process proc, request_block& req) const
    {
        standard_receiver re(comm, proc);
        re.irecv(*ptr, req);
    }

    template<typename Type>
    inline void intracomm::cirecv(Type* ptr, process proc, request_block& req, size_t condition_size) const
    {
        condition_receiver re(comm, proc, condition_size);
        re.irecv(*ptr, req);
    }

    template<typename Type>
    void intracomm::bcast(Type* ptr, process root) const
    {
        int comm_rank = rank(), comm_size = size();
        int my_pos = (comm_rank - root + comm_size) % comm_size;
        int i = 1;
        if (my_pos > 0)
        {
            for (int sum = 1; sum < my_pos; i <<= 1, sum += i);
            recv(ptr, (comm_rank - i + comm_size) % comm_size);
        }
        for (; i < comm_size; i <<= 1)
        {
            if ((my_pos < i) && (my_pos + i < comm_size))
                send(ptr, (comm_rank + i) % comm_size);
        }
    }

    template<typename Type>
    void intracomm::ibcast(Type* ptr, process root, request_block& req) const
    {
        int comm_rank = rank(), comm_size = size();
        int my_pos = (comm_rank - root + comm_size) % comm_size;
        int i = 1;
        if (my_pos > 0)
        {
            for (int sum = 1; sum < my_pos; i <<= 1, sum += i);
            irecv(ptr, (comm_rank - i + comm_size) % comm_size, req);
        }
        for (; i < comm_size; i <<= 1)
        {
            if ((my_pos < i) && (my_pos + i < comm_size))
            {
                req.wait_all();
                isend(ptr, (comm_rank + i) % comm_size, req);
            }
        }
    }

    class global_intracomm: public intracomm
    {
    public:

        global_intracomm();
        ~global_intracomm();

    };

}

#endif // __INTRACOMM_H__
