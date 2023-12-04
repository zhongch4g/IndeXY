#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

using namespace std;

// Queue into which producer adds data and from which the consumer picks data
queue<long> dataQ;

// first one is for the producer consumer operation, the second one to sync the prints
mutex pc_mu, print_mu;

// Conditional variable for the producer consumer sync
condition_variable consumerwait_cv;

void Print (string str) {
    // Let us have the Print also guarded so that the output appears sequential
    lock_guard<mutex> lg (print_mu);
    cout << str << endl;
}

void Producer () {
    int index = 0;
    while (1) {
        unique_lock<mutex> ul (pc_mu);

        // If more than 4 unprocessed items are in the data queue,
        // wait for sometime before adding more
        if (dataQ.size () >= 1) {
            consumerwait_cv.wait (ul, [] () { return !(dataQ.size () >= 1); });
        }

        dataQ.push (index);
        // Unlock the lock and notify the consumer that data is available
        ul.unlock ();
        consumerwait_cv.notify_one ();
        // Just adding some random delay
        this_thread::sleep_for (chrono::milliseconds (100));

        Print (" Producer produced " + to_string (index));

        index++;
    }
}

void Consumer () {
    while (1) {
        unique_lock<mutex> ul (pc_mu);
        if (dataQ.empty ())  // If the data queue is empty,
                             // wait for the producer to add something to it
        {
            // Predicate should return false to continue waiting.
            // Thus, if the queue is empty, predicate should return false (!q.empty())
            consumerwait_cv.wait (ul, [] () { return !dataQ.empty (); });
        }

        ul.unlock ();  // Unlock the lock to unblock the producer.
        // If this statement is commented, the producer is blocked till this loop ends
        int element = dataQ.front ();  // Pick the element from the queue
        dataQ.pop ();

        consumerwait_cv.notify_one ();  // Tell the producer that they can go ahead
        // since 1 element is now popped off for processing

        // Wait for some time to show that the consumer is slower than the producer
        this_thread::sleep_for (chrono::milliseconds (100));
        Print (" Consumer got " + to_string (element));
    }
}

int main () {
    thread prod (Producer);
    thread cons1 (Consumer);
    thread cons2 (Consumer);
    thread cons3 (Consumer);
    thread cons4 (Consumer);

    prod.detach ();
    cons1.detach ();
    cons2.detach ();
    cons3.detach ();
    cons4.detach ();

    // Wait in an infinite loop to see the producer consumer flow happen
    while (1)
        ;
    // this_thread::sleep_for (chrono::milliseconds (10000));
}