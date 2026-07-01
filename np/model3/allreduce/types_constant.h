
#ifndef __TYPES_CONSTANT_H__
#define __TYPES_CONSTANT_H__

#define EPIC_N_RANKS        4
#if defined(ONLY_FAIN_FANIN2)
#define EPIC_FANIN_FULL     2
#elif defined(ONLY_FAIN_FANIN3)
#define EPIC_FANIN_FULL     3
#elif defined(ONLY_FAIN_REDUCE)
#define EPIC_FANIN_FULL     3
#else
#define EPIC_FANIN_FULL     EPIC_N_RANKS
#endif

#define RANK1_IN_CHAN       220
#define RANK2_IN_CHAN       236
#define RANK3_IN_CHAN       0
#define RANK4_IN_CHAN       8

#define RANK1_OUT_CHAN      220
#define RANK1_OUT_SQID      440
#define RANK2_OUT_CHAN      236
#define RANK2_OUT_SQID      472
#define RANK3_OUT_CHAN      0
#define RANK3_OUT_SQID      0
#define RANK4_OUT_CHAN      8
#define RANK4_OUT_SQID      16

#define WORKER1_MAC_LO16    0x0095
#define WORKER2_MAC_LO16    0xD851
#define WORKER3_MAC_LO16    0xE441
#define WORKER4_MAC_LO16    0xD421

#define WORKER1_OUT_CHAN    RANK1_OUT_CHAN
#define WORKER1_OUT_SQID    RANK1_OUT_SQID
#define WORKER2_OUT_CHAN    RANK2_OUT_CHAN
#define WORKER2_OUT_SQID    RANK2_OUT_SQID
#define WORKER3_OUT_CHAN    RANK3_OUT_CHAN
#define WORKER3_OUT_SQID    RANK3_OUT_SQID
#define WORKER4_OUT_CHAN    RANK4_OUT_CHAN
#define WORKER4_OUT_SQID    RANK4_OUT_SQID

#define RANK1_IP            0xC0A80101
#define RANK2_IP            0xC0A80102
#define RANK3_IP            0xC0A80103
#define RANK4_IP            0xC0A80104

#define ROCE_UDP_PORT       4791

#define BTH_OP_RC_SEND_ONLY     0x04
#define BTH_OP_RC_SEND_ONLY_IMM 0x05
#define BTH_OP_RC_ACK           0x11
#define BTH_OP_RC_ACK_LO        0x11
#define BTH_OP_RC_ACK_HI        0x13
#define BTH_OP_UC_SEND_ONLY     0x24
#define BTH_OP_UD_SEND_ONLY     0x64
#define SYNC_FULL               3
#define BTH_OP_UC_SEND_FIRST    0x20
#define BTH_OP_UC_SEND_MIDDLE   0x21
#define BTH_OP_UC_SEND_LAST     0x22
#define BTH_OP_UC_SEND_LAST_IMM 0x23
#define BTH_OP_RC_SEND_FIRST    0x00
#define BTH_OP_RC_SEND_MIDDLE   0x01
#define BTH_OP_RC_SEND_LAST     0x02
#define BTH_OP_RC_SEND_LAST_IMM 0x03
#define BTH_OP_RC_WRITE_FIRST   0x06
#define BTH_OP_RC_WRITE_MIDDLE  0x07
#define BTH_OP_RC_WRITE_LAST    0x08
#define BTH_OP_RC_WRITE_ONLY    0x0A
#define BTH_OP_UC_WRITE_FIRST   0x26
#define BTH_OP_UC_WRITE_ONLY    0x2A

#ifndef ONLY_FAIN_ECHO
#define ONLY_FAIN_ECHO 0
#endif

#ifndef ONLY_FAIN_COMMON_QP
#define ONLY_FAIN_COMMON_QP 1
#endif

#define SWITCH_MAC          0xE8F9D4735665
#define VEP_IP              0xC0A801FA
#define VEP_QP              0x000EC1

#define ACK_IP_TOTAL_LEN    48
#define ACK_UDP_PKT_LEN     28

#define AETH_SYNDROME_ACK   0x1F

#define PRIORITY_ZERO       0
#define PRIORITY_ONE        1

#define EPIC_SLOT_N_WIDTH   16
#define EPIC_SLOT_N         (1 << EPIC_SLOT_N_WIDTH)
#define EPIC_SEG            32
#define EPIC_LANEBUF_SIZE   (EPIC_SEG * EPIC_SLOT_N)
#define EPIC_LANEBUF_WIDTH  21
#define EPIC_LANES          16

#define LANE_HI_BASE(k)     (((k) * 2)     * EPIC_SLOT_N)
#define LANE_LO_BASE(k)     (((k) * 2 + 1) * EPIC_SLOT_N)

#ifdef ONLY_FAIN_REDUCE_BIGSLOT
#define EPIC_FANIN_N_WIDTH  18
#else
#define EPIC_FANIN_N_WIDTH  EPIC_SLOT_N_WIDTH
#endif
#define EPIC_FANIN_SIZE     (1 << EPIC_FANIN_N_WIDTH)
#define EPIC_FANIN_N_MASK   (EPIC_FANIN_SIZE - 1)

#define EPIC_SLOT_N_MASK    (EPIC_SLOT_N - 1)

#define ROOT_IN_CHAN        RANK4_IN_CHAN
#define ROOT_IP             RANK4_IP
#define ROOT_OUT_CHAN       WORKER4_OUT_CHAN
#define ROOT_OUT_SQID       WORKER4_OUT_SQID
#define ROOT_MAC            0x1070FD2FD421

#define WORKER1_MAC         0x1070FD190095
#define WORKER2_MAC         0x1070FD2FD851
#define WORKER3_MAC         0x1070FD2FE441
#define WORKER4_MAC         0x1070FD2FD421

#define RANK1_DOWN_IP       RANK1_IP
#define RANK2_DOWN_IP       RANK2_IP
#define RANK3_DOWN_IP       RANK3_IP
#define RANK4_DOWN_IP       RANK4_IP

#define RESULT_IP_TOTAL_LEN 108
#define RESULT_UDP_PKT_LEN  88
#define EPIC_ROOTQP_SIZE    2
#define ROOTQP_LO           0
#define ROOTQP_HI           1

#define EPIC_RANKQP_SIZE    8
#define RANKQP_LO0          0
#define RANKQP_HI0          1
#define RANKQP_LO1          2
#define RANKQP_HI1          3
#define RANKQP_LO2          4
#define RANKQP_HI2          5
#define RANKQP_LO3          6
#define RANKQP_HI3          7

#define EPIC_SENDERQP_SEG       8
#define EPIC_SENDERQP_SIZE      (EPIC_SENDERQP_SEG * EPIC_SLOT_N)
#define EPIC_SENDERQP_WIDTH     19
#define SENDERQP_LO_BASE(r)     (((r) * 2)     * EPIC_SLOT_N)
#define SENDERQP_HI_BASE(r)     (((r) * 2 + 1) * EPIC_SLOT_N)

#define EPIC_NEP            EPIC_N_RANKS
#define EPIC_ARRIVED_SIZE   (EPIC_NEP * EPIC_SLOT_N)
#define EPIC_ARRIVED_WIDTH  18
#define ARRIVED_BASE0       (0 * EPIC_SLOT_N)
#define ARRIVED_BASE1       (1 * EPIC_SLOT_N)
#define ARRIVED_BASE2       (2 * EPIC_SLOT_N)
#define ARRIVED_BASE3       (3 * EPIC_SLOT_N)

#define EPIC_DROP_ACK_EVERY_K 0

#define EPIC_MW_WIDTH       (EPIC_SLOT_N_WIDTH - 1)
#define EPIC_MW             (1 << EPIC_MW_WIDTH)

#define EPIC_PSNRANGE_SIZE  4
#define PSNRANGE_FIRST_LO   0
#define PSNRANGE_FIRST_HI   1
#define PSNRANGE_LAST_LO    2
#define PSNRANGE_LAST_HI    3
#define EPIC_PSN_MAX        0xFFFFFF

#define DEBUG_CNTID_NUM             32
enum DEBUG_CNTID_E
{
    DEBUG_CNTID_RECV_PKTS         = 0,
    DEBUG_CNTID_UP_DATA           = 1,
    DEBUG_CNTID_DUP               = 2,
    DEBUG_CNTID_DROP_RANGE        = 3,
    DEBUG_CNTID_DROP_OTHER_UDP    = 4,
    DEBUG_CNTID_DROP_WRONG_INCHAN = 5,
    DEBUG_CNTID_DROP_NON_ETH      = 6,
    DEBUG_CNTID_DROP_NON_ROCE     = 7,
    DEBUG_CNTID_AGG               = 8,
    DEBUG_CNTID_BUFFERED          = 9,
    DEBUG_CNTID_TX_RESULT         = 10,
    DEBUG_CNTID_TX_FANOUT         = 11,
    DEBUG_CNTID_TX_ACK            = 12,
    DEBUG_CNTID_WIPE              = 13,
    DEBUG_CNTID_TX_FWD            = 14,
    DEBUG_CNTID_SKEW_ERR          = 15,
    DEBUG_CNTID_MAC_MISS          = 16,
    DEBUG_CNTID_DROP_ACK_TICK     = 17,
    DEBUG_CNTID_DROP_ACK_DONE     = 18,
    DEBUG_CNTID_TX_CTRL           = 19,
    DEBUG_CNTID_REACK             = 20,
    DEBUG_CNTID_SYNC_IN           = 21,
    DEBUG_CNTID_SYNC_REL          = 22,
    DEBUG_CNTID_MAX               = (DEBUG_CNTID_NUM - 1)
};

#endif
