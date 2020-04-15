#ifndef __TRANSFER_H__
#define __TRANSFER_H__

#include <queue>
#include <climits>
#include "mpi.h"
#include "parallel_defs.h"

namespace apl
{

    class sender
    {
    protected:

        mutable int tag;

    public:

        sender();
        virtual ~sender();

        virtual void send(const void* buf, int size, MPI_Datatype type) const = 0;
        virtual void isend(const void* buf, int size, MPI_Datatype type) const = 0;

        void send_bytes(const void* buf, int count) const;
        void isend_bytes(const void* buf, int count) const;

        template<class T>
        void send(const T* buf, int size = 1) const;
        template<class T>
        void isend(const T* buf, int size = 1) const;

        virtual void wait_all() const = 0;

    };

    class receiver
    {
    protected:

        mutable int tag;

    public:

        receiver();
        virtual ~receiver();

        virtual void recv(void* buf, int size, MPI_Datatype type) const = 0;
        virtual void irecv(void* buf, int size, MPI_Datatype type) const = 0;
        virtual int probe(MPI_Datatype type) const = 0;

        void recv_bytes(void* buf, int count) const;
        void irecv_bytes(void* buf, int count) const;
        int probe_bytes() const;

        template<class T>
        void recv(T* buf, int size = 1) const;
        template<class T>
        void irecv(T* buf, int size = 1) const;
        template<class T>
        int probe() const;

        virtual void wait_all() const = 0;

    };

    //send-recv specifications
    // char
    template<>
    void sender::send<char>(const char* buf, int size) const;

    template<>
    void sender::isend<char>(const char* buf, int size) const;

    template<>
    void receiver::recv<char>(char* buf, int size) const;

    template<>
    void receiver::irecv<char>(char* buf, int size) const;

    template<>
    int receiver::probe<char>() const;

    // unsigned char
    template<>
    void sender::send<unsigned char>(const unsigned char* buf, int size) const;

    template<>
    void sender::isend<unsigned char>(const unsigned char* buf, int size) const;

    template<>
    void receiver::recv<unsigned char>(unsigned char* buf, int size) const;

    template<>
    void receiver::irecv<unsigned char>(unsigned char* buf, int size) const;

    template<>
    int receiver::probe<unsigned char>() const;

    // short
    template<>
    void sender::send<short>(const short* buf, int size) const;

    template<>
    void sender::isend<short>(const short* buf, int size) const;

    template<>
    void receiver::recv<short>(short* buf, int size) const;

    template<>
    void receiver::irecv<short>(short* buf, int size) const;

    template<>
    int receiver::probe<short>() const;

    // unsigned short
    template<>
    void sender::send<unsigned short>(const unsigned short* buf, int size) const;

    template<>
    void sender::isend<unsigned short>(const unsigned short* buf, int size) const;

    template<>
    void receiver::recv<unsigned short>(unsigned short* buf, int size) const;

    template<>
    void receiver::irecv<unsigned short>(unsigned short* buf, int size) const;

    template<>
    int receiver::probe<unsigned short>() const;

    // int
    template<>
    void sender::send<int>(const int* buf, int size) const;

    template<>
    void sender::isend<int>(const int* buf, int size) const;

    template<>
    void receiver::recv<int>(int* buf, int size) const;

    template<>
    void receiver::irecv<int>(int* buf, int size) const;

    template<>
    int receiver::probe<int>() const;

    // unsigned
    template<>
    void sender::send<unsigned>(const unsigned* buf, int size) const;

    template<>
    void sender::isend<unsigned>(const unsigned* buf, int size) const;

    template<>
    void receiver::recv<unsigned>(unsigned* buf, int size) const;

    template<>
    void receiver::irecv<unsigned>(unsigned* buf, int size) const;

    template<>
    int receiver::probe<unsigned>() const;

    // long
    template<>
    void sender::send<long>(const long* buf, int size) const;

    template<>
    void sender::isend<long>(const long* buf, int size) const;

    template<>
    void receiver::recv<long>(long* buf, int size) const;

    template<>
    void receiver::irecv<long>(long* buf, int size) const;

    template<>
    int receiver::probe<long>() const;

    // unsigned long
    template<>
    void sender::send<unsigned long>(const unsigned long* buf, int size) const;

    template<>
    void sender::isend<unsigned long>(const unsigned long* buf, int size) const;

    template<>
    void receiver::recv<unsigned long>(unsigned long* buf, int size) const;

    template<>
    void receiver::irecv<unsigned long>(unsigned long* buf, int size) const;

    template<>
    int receiver::probe<unsigned long>() const;

    // long long
    template<>
    void sender::send<long long>(const long long* buf, int size) const;

    template<>
    void sender::isend<long long>(const long long* buf, int size) const;

    template<>
    void receiver::recv<long long>(long long* buf, int size) const;

    template<>
    void receiver::irecv<long long>(long long* buf, int size) const;

    template<>
    int receiver::probe<long long>() const;

    // unsigned long long
    template<>
    void sender::send<unsigned long long>(const unsigned long long* buf, int size) const;

    template<>
    void sender::isend<unsigned long long>(const unsigned long long* buf, int size) const;

    template<>
    void receiver::recv<unsigned long long>(unsigned long long* buf, int size) const;

    template<>
    void receiver::irecv<unsigned long long>(unsigned long long* buf, int size) const;

    template<>
    int receiver::probe<unsigned long long>() const;

    // float
    template<>
    void sender::send<float>(const float* buf, int size) const;

    template<>
    void sender::isend<float>(const float* buf, int size) const;

    template<>
    void receiver::recv<float>(float* buf, int size) const;

    template<>
    void receiver::irecv<float>(float* buf, int size) const;

    template<>
    int receiver::probe<float>() const;

    // double
    template<>
    void sender::send<double>(const double* buf, int size) const;

    template<>
    void sender::isend<double>(const double* buf, int size) const;

    template<>
    void receiver::recv<double>(double* buf, int size) const;

    template<>
    void receiver::irecv<double>(double* buf, int size) const;

    template<>
    int receiver::probe<double>() const;

    // long double
    template<>
    void sender::send<long double>(const long double* buf, int size) const;

    template<>
    void sender::isend<long double>(const long double* buf, int size) const;

    template<>
    void receiver::recv<long double>(long double* buf, int size) const;

    template<>
    void receiver::irecv<long double>(long double* buf, int size) const;

    template<>
    int receiver::probe<long double>() const;

}

#endif // __TRANSFER_H__
