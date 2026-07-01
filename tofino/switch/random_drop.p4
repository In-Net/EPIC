#ifndef __RANDOM_DROP_P4__
#define __RANDOM_DROP_P4__
// for DEBUG

control compare(in bit<32> x, in bit<32> y, out bit<2> ret) {// return 0 on ==, 1 on >, -1(3) on < 
    action set_ret(bit<2> _ret) {
        ret = _ret;
    }
    table compare_table {
        key = {
            x: ternary;
            y: ternary;
        }
        actions = {
            set_ret;
        }
        const entries = {
#define ZERO_ON_BIT(k) 0 &&& 1<<k
#define ONE_ON_BIT(k) 1<<k &&& 1<<k
#define CHECK_BIT(k) (ZERO_ON_BIT(k), ONE_ON_BIT(k)): set_ret(3);\
(ONE_ON_BIT(k), ZERO_ON_BIT(k)): set_ret(1)

            CHECK_BIT(31);
            CHECK_BIT(30);
            CHECK_BIT(29);
            CHECK_BIT(28);
            CHECK_BIT(27);
            CHECK_BIT(26);
            CHECK_BIT(25);
            CHECK_BIT(24);
            CHECK_BIT(23);
            CHECK_BIT(22);
            CHECK_BIT(21);
            CHECK_BIT(20);
            CHECK_BIT(19);
            CHECK_BIT(18);
            CHECK_BIT(17);
            CHECK_BIT(16);
            CHECK_BIT(15);
            CHECK_BIT(14);
            CHECK_BIT(13);
            CHECK_BIT(12);
            CHECK_BIT(11);
            CHECK_BIT(10);
            CHECK_BIT(9);
            CHECK_BIT(8);
            CHECK_BIT(7);
            CHECK_BIT(6);
            CHECK_BIT(5);
            CHECK_BIT(4);
            CHECK_BIT(3);
            CHECK_BIT(2);
            CHECK_BIT(1);
            CHECK_BIT(0);
            // (_, _): set_ret(0);

#undef CHECK_BIT
#undef ONE_ON_BIT
#undef ZERO_ON_BIT
        }
        const default_action = set_ret(0);
        size = 65;
    }
    apply {
        compare_table.apply();
    }
} 

control random_drop(inout egress_intrinsic_metadata_for_deparser_t eg_dps_md) {

    bit<32> random_value;
    bit<32> drop_value;// if random_value < drop_value, the packet will be dropped
    bit<2> cmp_res;

    Random<bit<32>>() random_generator;

    action get_probability(bit<32> p) {// the real probability is p/2**32
        drop_value = p;
    }

    table drop_probability_table {
        actions = {
            get_probability;
        }
        default_action = get_probability(0);
        size = 1;
    }

    action get_random() {
        random_value = random_generator.get();
    }

    action drop() {
        eg_dps_md.drop_ctl = 1;
    }

    action no_drop() {
        eg_dps_md.drop_ctl = 0;
    }

    apply {
        get_random();
        drop_probability_table.apply();
        compare.apply(random_value, drop_value, cmp_res);
        if(cmp_res == 3) drop();// random_value < drop_value
        else no_drop();
    }
}

#endif