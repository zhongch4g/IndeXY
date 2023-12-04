#include <jemalloc/jemalloc.h>

#include "artree-shared-headers/Key.h"
using namespace ART;
class TransactionRead;
struct ModifyTarget;
struct IndexInfo;
struct warehouse_t {
    static constexpr int id = 0;
    struct Key {
        static constexpr int id = 0;
        Integer w_id;
    };
    Key key;
    Varchar<10> w_name;
    Varchar<20> w_street_1;
    Varchar<20> w_street_2;
    Varchar<20> w_city;
    Varchar<2> w_state;
    Varchar<9> w_zip;
    Numeric w_tax;
    Numeric w_ytd;
    // -------------------------------------------------------------------------------------
    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (warehouse_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.w_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (warehouse_t::Key));
        warehouse_t* wh = reinterpret_cast<warehouse_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (wh->key.w_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (warehouse_t::Key));
        warehouse_t* wh = reinterpret_cast<warehouse_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (wh->key.w_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("warehouse_t w_id = %u tax %2.1f ytd = %2.1f\n", record->key.w_id, record->w_tax,
                record->w_ytd);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (warehouse_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        warehouse_t* wh = new warehouse_t ();
        memcpy (wh, rValue, sizeof (warehouse_t));
        rid = reinterpret_cast<uint64_t> (wh);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        warehouse_t* wh = reinterpret_cast<warehouse_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (wh));
        delete wh;
    }
};

struct district_t {
    static constexpr int id = 1;
    struct Key {
        static constexpr int id = 1;
        Integer d_w_id;
        Integer d_id;
    };
    Key key;
    Varchar<10> d_name;
    Varchar<20> d_street_1;
    Varchar<20> d_street_2;
    Varchar<20> d_city;
    Varchar<2> d_state;
    Varchar<9> d_zip;
    Numeric d_tax;
    Numeric d_ytd;
    Integer d_next_o_id;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (district_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.d_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.d_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (district_t::Key));
        district_t* dist = reinterpret_cast<district_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (dist->key.d_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (dist->key.d_id);
        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (district_t::Key));
        district_t* dist = reinterpret_cast<district_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (dist->key.d_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (dist->key.d_id);
        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("district_t To String d_w_id = %u d_id = %u ytd = %2.1f nid = %u\n",
                record->key.d_w_id, record->key.d_id, record->d_ytd, record->d_next_o_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (district_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        district_t* dist = new district_t ();
        memcpy (dist, rValue, sizeof (district_t));
        rid = reinterpret_cast<uint64_t> (dist);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        district_t* dist = reinterpret_cast<district_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (dist));
        delete dist;
    }
};

struct customer_t {
    static constexpr int id = 2;
    struct Key {
        static constexpr int id = 2;
        Integer c_w_id;
        Integer c_d_id;
        Integer c_id;
    };
    Key key;
    Varchar<16> c_first;
    Varchar<2> c_middle;
    Varchar<16> c_last;
    Varchar<20> c_street_1;
    Varchar<20> c_street_2;
    Varchar<20> c_city;
    Varchar<2> c_state;
    Varchar<9> c_zip;
    Varchar<16> c_phone;
    Timestamp c_since;
    Varchar<2> c_credit;
    Numeric c_credit_lim;
    Numeric c_discount;
    Numeric c_balance;
    Numeric c_ytd_payment;
    Numeric c_payment_cnt;
    Numeric c_delivery_cnt;
    Varchar<500> c_data;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (customer_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.c_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.c_d_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[2] = __builtin_bswap32 (key.c_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (customer_t::Key));
        customer_t* cust = reinterpret_cast<customer_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (cust->key.c_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (cust->key.c_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (cust->key.c_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (customer_t::Key));
        customer_t* cust = reinterpret_cast<customer_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (cust->key.c_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (cust->key.c_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (cust->key.c_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("customer_t To String c_w_id = %u\n", record->key.c_w_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (customer_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        customer_t* cust = new customer_t ();
        memcpy (cust, rValue, sizeof (customer_t));
        rid = reinterpret_cast<uint64_t> (cust);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        customer_t* cust = reinterpret_cast<customer_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (cust));
        delete cust;
    }
};

struct customer_wdl_t {
    static constexpr int id = 3;
    struct Key {
        static constexpr int id = 3;
        Integer c_w_id;
        Integer c_d_id;
        Varchar<16> c_last;
        Varchar<16> c_first;
    };
    Key key;
    Integer c_id;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (customer_wdl_t::Key));

        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.c_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.c_d_id);

        char* last = reinterpret_cast<char*> (&index_key[0] + 2 * sizeof (uint32_t));
        memset (last, 0, 17);
        memcpy (last, key.c_last.data, key.c_last.length);
        assert (last[16] == '\0');

        last = last + 17;
        memset (last, 0, 17);
        memcpy (last, key.c_first.data, key.c_first.length);
        assert (last[16] == '\0');
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (customer_wdl_t::Key));
        customer_wdl_t* cust_wdl = reinterpret_cast<customer_wdl_t*> (tid);

        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (cust_wdl->key.c_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (cust_wdl->key.c_d_id);

        char* last = reinterpret_cast<char*> (&akey[0] + 2 * sizeof (uint32_t));
        memset (last, 0, 17);
        memcpy (last, cust_wdl->key.c_last.data, cust_wdl->key.c_last.length);
        assert (last[16] == '\0');

        last = last + 17;
        memset (last, 0, 17);
        memcpy (last, cust_wdl->key.c_first.data, cust_wdl->key.c_first.length);
        assert (last[16] == '\0');

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (customer_wdl_t::Key));
        customer_wdl_t* cust_wdl = reinterpret_cast<customer_wdl_t*> (tid);

        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (cust_wdl->key.c_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (cust_wdl->key.c_d_id);

        char* last = reinterpret_cast<char*> (&akey[0] + 2 * sizeof (uint32_t));
        memset (last, 0, 17);
        memcpy (last, cust_wdl->key.c_last.data, cust_wdl->key.c_last.length);
        assert (last[16] == '\0');

        last = last + 17;
        memset (last, 0, 17);
        memcpy (last, cust_wdl->key.c_first.data, cust_wdl->key.c_first.length);
        assert (last[16] == '\0');

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("customer_wdl_t To String c_w_id = %u\n", record->key.c_w_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (customer_wdl_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        customer_wdl_t* custw = new customer_wdl_t ();
        memcpy (custw, rValue, sizeof (customer_wdl_t));
        rid = reinterpret_cast<uint64_t> (custw);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        customer_wdl_t* custw = reinterpret_cast<customer_wdl_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (custw));
        delete custw;
    }
};

struct history_t {
    static constexpr int id = 4;
    struct Key {
        static constexpr int id = 4;
        Integer thread_id;
        Integer h_pk;
    };
    Key key;
    Integer h_c_id;
    Integer h_c_d_id;
    Integer h_c_w_id;
    Integer h_d_id;
    Integer h_w_id;
    Timestamp h_date;
    Numeric h_amount;
    Varchar<24> h_data;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (history_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.thread_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.h_pk);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (history_t::Key));
        history_t* hist = reinterpret_cast<history_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (hist->key.thread_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (hist->key.h_pk);
        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (history_t::Key));
        history_t* hist = reinterpret_cast<history_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (hist->key.thread_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (hist->key.h_pk);
        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("history_t To String thread_id = %u\n", record->key.thread_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (history_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        history_t* hist = new history_t ();
        memcpy (hist, rValue, sizeof (history_t));
        rid = reinterpret_cast<uint64_t> (hist);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        history_t* hist = reinterpret_cast<history_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (hist));
        delete hist;
    }
};

struct neworder_t {
    static constexpr int id = 5;
    struct Key {
        static constexpr int id = 5;
        Integer no_w_id;
        Integer no_d_id;
        Integer no_o_id;
    };
    Key key;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (neworder_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.no_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.no_d_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[2] = __builtin_bswap32 (key.no_o_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (neworder_t::Key));
        neworder_t* no = reinterpret_cast<neworder_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (no->key.no_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (no->key.no_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (no->key.no_o_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (neworder_t::Key));
        neworder_t* no = reinterpret_cast<neworder_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (no->key.no_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (no->key.no_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (no->key.no_o_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("new_order_t To String no_w_id = %u\n", record->key.no_w_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (neworder_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        neworder_t* no = new neworder_t ();
        memcpy (no, rValue, sizeof (neworder_t));
        rid = reinterpret_cast<uint64_t> (no);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        neworder_t* no = reinterpret_cast<neworder_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (no));
        delete no;
    }
};

struct order_t {
    static constexpr int id = 6;
    struct Key {
        static constexpr int id = 6;
        Integer o_w_id;
        Integer o_d_id;
        Integer o_id;
    };
    Key key;
    Integer o_c_id;
    Timestamp o_entry_d;
    Integer o_carrier_id;
    Numeric o_ol_cnt;
    Numeric o_all_local;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (order_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.o_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.o_d_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[2] = __builtin_bswap32 (key.o_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (order_t::Key));
        order_t* odr = reinterpret_cast<order_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (odr->key.o_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (odr->key.o_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (odr->key.o_id);
        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (order_t::Key));
        order_t* odr = reinterpret_cast<order_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (odr->key.o_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (odr->key.o_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (odr->key.o_id);
        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("To String o_w_id = %u o_d_id %u o_id %u \n", record->key.o_w_id,
                record->key.o_d_id, record->key.o_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (order_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        order_t* od = new order_t ();
        memcpy (od, rValue, sizeof (order_t));
        rid = reinterpret_cast<uint64_t> (od);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        order_t* od = reinterpret_cast<order_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (od));
        delete od;
    }
};

struct order_wdc_t {
    static constexpr int id = 7;
    struct Key {
        static constexpr int id = 7;
        Integer o_w_id;
        Integer o_d_id;
        Integer o_c_id;
        Integer o_id;
    };
    Key key;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (order_wdc_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.o_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.o_d_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[2] = __builtin_bswap32 (key.o_c_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[3] = __builtin_bswap32 (key.o_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (order_wdc_t::Key));
        order_wdc_t* or_wdc = reinterpret_cast<order_wdc_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (or_wdc->key.o_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (or_wdc->key.o_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (or_wdc->key.o_c_id);
        reinterpret_cast<uint32_t*> (&akey[0])[3] = __builtin_bswap32 (or_wdc->key.o_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (order_wdc_t::Key));
        order_wdc_t* or_wdc = reinterpret_cast<order_wdc_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (or_wdc->key.o_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (or_wdc->key.o_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (or_wdc->key.o_c_id);
        reinterpret_cast<uint32_t*> (&akey[0])[3] = __builtin_bswap32 (or_wdc->key.o_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("To String o_w_id = %u o_d_id %u o_id %u \n", record->key.o_w_id,
                record->key.o_d_id, record->key.o_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (order_wdc_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        order_wdc_t* odw = new order_wdc_t ();
        memcpy (odw, rValue, sizeof (order_wdc_t));
        rid = reinterpret_cast<uint64_t> (odw);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        order_wdc_t* odw = reinterpret_cast<order_wdc_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (odw));
        delete odw;
    }
};

struct orderline_t {
    static constexpr int id = 8;
    struct Key {
        static constexpr int id = 8;
        Integer ol_w_id;
        Integer ol_d_id;
        Integer ol_o_id;
        Integer ol_number;
    };
    Key key;
    Integer ol_i_id;
    Integer ol_supply_w_id;
    Timestamp ol_delivery_d;
    Numeric ol_quantity;
    Numeric ol_amount;
    Varchar<24> ol_dist_info;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (orderline_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.ol_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.ol_d_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[2] = __builtin_bswap32 (key.ol_o_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[3] = __builtin_bswap32 (key.ol_number);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (orderline_t::Key));
        orderline_t* ol = reinterpret_cast<orderline_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (ol->key.ol_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (ol->key.ol_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (ol->key.ol_o_id);
        reinterpret_cast<uint32_t*> (&akey[0])[3] = __builtin_bswap32 (ol->key.ol_number);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (orderline_t::Key));
        orderline_t* ol = reinterpret_cast<orderline_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (ol->key.ol_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (ol->key.ol_d_id);
        reinterpret_cast<uint32_t*> (&akey[0])[2] = __builtin_bswap32 (ol->key.ol_o_id);
        reinterpret_cast<uint32_t*> (&akey[0])[3] = __builtin_bswap32 (ol->key.ol_number);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("orderline To String o_w_id = %u o_d_id %u o_id %u \n", record->key.ol_w_id,
                record->key.ol_d_id, record->key.ol_o_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (orderline_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        orderline_t* ol = new orderline_t ();
        memcpy (ol, rValue, sizeof (orderline_t));
        rid = reinterpret_cast<uint64_t> (ol);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        orderline_t* ol = reinterpret_cast<orderline_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (ol));
        delete ol;
    }
};

struct item_t {
    static constexpr int id = 9;
    struct Key {
        static constexpr int id = 9;
        Integer i_id;
    };
    Key key;
    Integer i_im_id;
    Varchar<24> i_name;
    Numeric i_price;
    Varchar<50> i_data;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (item_t::Key));
        reinterpret_cast<int*> (&index_key[0])[0] = __builtin_bswap32 (key.i_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (item_t::Key));
        item_t* it = reinterpret_cast<item_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (it->key.i_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (item_t::Key));
        item_t* it = reinterpret_cast<item_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (it->key.i_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("item To String i_id = %u\n", record->key.i_id);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (item_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        item_t* it = new item_t ();
        memcpy (it, rValue, sizeof (item_t));
        rid = reinterpret_cast<uint64_t> (it);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        item_t* it = reinterpret_cast<item_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (it));
        delete it;
    }
};

struct stock_t {
    static constexpr int id = 10;
    struct Key {
        static constexpr int id = 10;
        Integer s_w_id;
        Integer s_i_id;
    };
    Key key;
    Numeric s_quantity;
    Varchar<24> s_dist_01;
    Varchar<24> s_dist_02;
    Varchar<24> s_dist_03;
    Varchar<24> s_dist_04;
    Varchar<24> s_dist_05;
    Varchar<24> s_dist_06;
    Varchar<24> s_dist_07;
    Varchar<24> s_dist_08;
    Varchar<24> s_dist_09;
    Varchar<24> s_dist_10;
    Numeric s_ytd;
    Numeric s_order_cnt;
    Numeric s_remote_cnt;
    Varchar<50> s_data;

    template <class T>
    static void TransformKey (const T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (stock_t::Key));
        reinterpret_cast<uint32_t*> (&index_key[0])[0] = __builtin_bswap32 (key.s_w_id);
        reinterpret_cast<uint32_t*> (&index_key[0])[1] = __builtin_bswap32 (key.s_i_id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (stock_t::Key));
        stock_t* stk = reinterpret_cast<stock_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (stk->key.s_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (stk->key.s_i_id);

        return true;
    }

    static bool loadKeyOri (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (stock_t::Key));
        stock_t* stk = reinterpret_cast<stock_t*> (tid);
        reinterpret_cast<uint32_t*> (&akey[0])[0] = __builtin_bswap32 (stk->key.s_w_id);
        reinterpret_cast<uint32_t*> (&akey[0])[1] = __builtin_bswap32 (stk->key.s_i_id);

        return true;
    }

    template <class T>
    static void ToString (const T* record) {
        printf ("stock_t s_w_id %u s_i_id %u s_ytd %2.1f s_order_cnt %2.1f s_remote_cnt %2.1f\n",
                record->key.s_w_id, record->key.s_i_id, record->s_ytd, record->s_order_cnt,
                record->s_remote_cnt);
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        rValue = std::string ((char*)rid, sizeof (stock_t));
    }

    static void getRid (uint64_t& rid, char* rValue) {
        stock_t* st = new stock_t ();
        memcpy (st, rValue, sizeof (stock_t));
        rid = reinterpret_cast<uint64_t> (st);
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        stock_t* st = reinterpret_cast<stock_t*> (rid);
        deallocated.fetch_add (malloc_usable_size (st));
        delete st;
    }
};
