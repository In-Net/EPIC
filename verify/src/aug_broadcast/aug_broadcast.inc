configuration {
    // Property.
    TERMINATION_CHECK = FALSE;

    // Network nondeterminism.
    MAX_LOSS = 0;
    MAX_OUT_OF_ORDER = 0;
    MAX_DUPLICATION = 0;

    // System parameters.
    AGG_NUM = 1;
    REQ_NUM = 1;
    WINDOW_SIZE = 1;
    FANIN = 2;

    // Packet types.
    RES = 1;
    ACK = 2;
    NAK = 3;

    // Other constants.
    ROOT = 1;      // Root switch of reduction tree.
    COMM_ROOT = 7; // Root client of communication group.
}

topology {
    nodetype Client, Switch;
    node(Switch) r = 1, s12 = 2, s34 = 3;
    node(Client) c1 = 4, c2 = 5, c3 = 6, c4 = 7;
    link(unreliable) r -- s12, s34;
    link(unreliable) s12 -- c1, c2;
    link(unreliable) s34 -- c3, c4;
}

protocol {
    // Sets.
    const(all) REQ_SET = 0..(REQ_NUM - 1);
    const(all) AGG_SET = 0..(AGG_NUM - 1);

    // Helper functions.
    fn SmallerOf(a, b) = IF a < b THEN a ELSE b;
    fn LargerOf(a, b) = IF a > b THEN a ELSE b;
    fn ParentOf(nid) = __next_hop[nid, COMM_ROOT];
    fn ChildrenOf(nid) = {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0};
    fn MakePkt(type, src, dst, psn) = [
        type |-> type,
        src |-> src,
        dst |-> dst,
        psn |-> psn
    ];
    fn MakePktWithValue(type, src, dst, psn, value) = [
        type |-> type,
        src |-> src,
        dst |-> dst,
        psn |-> psn,
        value |-> value
    ];
    fn Idx(psn) = psn % AGG_NUM;
    fn IsCompleted(nid, epsn, psn) = \A ch \in ChildrenOf(nid) : epsn[ch] > psn;
    fn MinEPsn(nid, epsn) = Min({epsn[ch] : ch \in ChildrenOf(nid)});
    fn SwitchCanTerminate(nid) = \A c \in NODE_SET : c > nid => __NodeTerminated(c);
    fn StrictRetxCondition(pkt) = \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>;
    fn PsnSetToIdxSet(range) = {Idx(psn) : psn \in range};
    fn RangeToSeq(start, stop) = [i \in 1..(stop - start + 1) |-> i + start - 1];
    fn MergeFuncOfSeq(fos) =
        LET
            RECURSIVE F(_)
            F(fos_) == IF DOMAIN fos_ = {} THEN <<>> ELSE
                LET d == CHOOSE x \in DOMAIN fos_: TRUE
                IN F([x \in DOMAIN fos_ \ {d} |-> fos_[x]]) \o fos_[d]
        IN F(fos);

    // Client requests.
    var(all) res = [i \in REQ_SET |-> RandomElement(0..100000)];
    var(all) requests = [i \in REQ_SET |->
        MakePktWithValue(RES, COMM_ROOT, __next_hop[COMM_ROOT, ROOT], i, res[i])];

    // Client states.
    var(all) base = -1;
    var(Client) res_epsn = 0;

    // Switch states.
    var(Switch) BEPsn = 0;
    var(Switch) BBuffer = [i \in AGG_SET |-> 0];
    var(Switch) BPsnStart = 0;
    var(Switch) REPsn = [ch \in ChildrenOf(self) |-> 0];
    var(Switch) nak_sent = FALSE;
    var(Switch) recirc_pkts = <<>>;

    // Helper macros.

    macro __SendNewPkt(pkt) {
        // We did not use the deduplication mechanism provided by VeriNC.
        temp next = __next_hop[self, pkt.dst];
        if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], pkt)) {
            send(pkt);
        }
    }

    macro SendNewPkt(type, src, dst, psn) {
        __SendNewPkt(MakePkt(type, src, dst, psn));
    }
    macro SendNewPktWithValue(type, src, dst, psn, value) {
        __SendNewPkt(MakePktWithValue(type, src, dst, psn, value));
    }

    // Client handlers.

    macro ClientAck(pkt) {
        assert(self = COMM_ROOT, "ClientAck: non-root client receives ACK.");
        if (pkt.psn > base) {
            temp start_idx = base + WINDOW_SIZE;
            temp end_idx = SmallerOf(pkt.psn + WINDOW_SIZE, REQ_NUM);
            if (start_idx < end_idx) {
                unicast([i \in start_idx..(end_idx - 1) |-> requests[i]]);
            }
            base = pkt.psn;
            if (base = REQ_NUM) {
                exit();
            }
        }
    }

    macro ClientNak(pkt) {
        assert(self = COMM_ROOT, "ClientNak: non-root client receives NAK.");
        if (pkt.psn >= base) {
            __SendNewPkt(requests[pkt.psn]);
        }
    }

    macro ClientRes(pkt) {
        assert(self # COMM_ROOT, "ClientRes: root client receives RES.");
        assert(pkt.value = res[pkt.psn], "ClientRes: incorrect result.");
        if (pkt.psn = res_epsn) {
            res_epsn = res_epsn + 1;
            SendNewPkt(ACK, self, pkt.src, res_epsn);
            if (res_epsn = REQ_NUM) {
                exit();
            }
        }
    }

    // Helper macros used by switch handlers.

    macro Recirc(pkt) {
        recirc_pkts = Append(recirc_pkts, pkt);
    }
    macro RecircMultiple(pkts) {
        recirc_pkts = recirc_pkts \o pkts;
    }

    // Switch handlers.

    macro SwitchRes(pkt) {
        assert(pkt.src = ParentOf(self), "SwitchRes: switch receives RES from non-parent.");
        temp idx = Idx(pkt.psn);
        if (pkt.psn < BEPsn) { // Behind.
            SendNewPkt(ACK, self, pkt.src, BEPsn);
        }
        elif (pkt.psn > BEPsn) { // Ahead.
            if (~nak_sent) {
                SendNewPkt(NAK, self, pkt.src, BEPsn);
                nak_sent = TRUE;
            }
        }
        else { // Sync.
            nak_sent = FALSE;
            if (pkt.psn >= BPsnStart + AGG_NUM) { // Buffer overflow.
                // Just drop it.
                ;
            }
            else {
                BBuffer[idx] = pkt.value;
                BEPsn = BEPsn + 1;
                multicast(MakePktWithValue(RES, self, self, pkt.psn, pkt.value), ChildrenOf(self)); // dst is dummy here.
                Recirc(MakePkt(ACK, self, pkt.src, BEPsn));
            }
        }
    }

    macro SwitchAck(pkt) {
        temp idx = Idx(pkt.psn);
        temp ch = pkt.src;
        assert(ch \in ChildrenOf(self), "SwitchAck: switch receives ACK from non-child.");
        assert(BEPsn >= pkt.psn, "SwitchAck: switch receives ACK for unreceived result.");
        if (pkt.psn > REPsn[ch]) {
            temp new_start = MinEPsn(self, ch :> pkt.psn @@ REPsn);
            temp range = BPsnStart..(new_start - 1);
            REPsn[ch] = pkt.psn;
            if (new_start > BPsnStart) {
                BBuffer = [i \in PsnSetToIdxSet(range) |-> 0] @@ BBuffer;
                BPsnStart = new_start;
            }
        }
    }

    macro SwitchNak(pkt) {
        temp idx = Idx(pkt.psn);
        temp ch = pkt.src;
        assert(ch \in ChildrenOf(self), "SwitchNak: switch receives NAK from non-child.");
        assert(pkt.psn = REPsn[ch], "SwitchNak: we currently do not consider out-of-order networks.");
        temp max_psn = BEPsn - 1;
        temp range_seq = RangeToSeq(REPsn[ch], max_psn);
        temp pkts = [i \in 1..Len(range_seq) |-> 
            MakePktWithValue(RES, self, ch, range_seq[i], BBuffer[Idx(range_seq[i])])];
        RecircMultiple(pkts);
    }

    // Threads.

    thread(Client) ClientInitSend {
    L_ClientInitSend:
        if (self = COMM_ROOT) {
            temp end_idx = SmallerOf(WINDOW_SIZE, REQ_NUM);
            unicast([i \in 0..(end_idx - 1) |-> requests[i]]);
            base = 0;
        }
    }

    thread(Client) ClientRecv {
    L_ClientRecv:
        while (TRUE) {
            temp pkt = receive();
            if (pkt.type = ACK) {
                ClientAck(pkt);
            } elif (pkt.type = NAK) {
                ClientNak(pkt);
            } elif (pkt.type = RES) {
                ClientRes(pkt);
            } else {
                assert(FALSE, "ClientRecv: unexpected packet type");
            }
        }
    }

    thread(Client) ClientRetx {
    L_ClientRetx:
        while (self = COMM_ROOT /\ base < REQ_NUM) {
            wait(base >= 0);
            wait(StrictRetxCondition(0));
            temp end_idx = SmallerOf(base + WINDOW_SIZE, REQ_NUM);
            unicast([i \in base..(end_idx - 1) |-> requests[i]]);
        }
    }

    thread(Switch) SwitchRecv {
    L_SwitchRecv:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts = <<>>);
            temp pkt = receive();
            if (pkt.type = RES) {
                SwitchRes(pkt);
            } elif (pkt.type = ACK) {
                SwitchAck(pkt);
            } elif (pkt.type = NAK) {
                SwitchNak(pkt);
            } else {
                assert(FALSE, "SwitchRecv: unexpected packet type");
            }
        }
    }

    thread(Switch) SwitchRecirc {
    L_SwitchRecirc:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts # <<>>);
            __SendNewPkt(Head(recirc_pkts));
            recirc_pkts = Tail(recirc_pkts);
        }
    }

    thread(Switch) SwitchRetxRes {
    L_SwitchRetxRes:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts = <<>>);
            wait(StrictRetxCondition(0));
            temp max_psn = BEPsn - 1;
            temp active_children = {ch \in ChildrenOf(self) : ~__NodeTerminated(ch)};
            temp range_seq = [ch \in active_children |-> RangeToSeq(REPsn[ch], max_psn)];
            temp pkts = MergeFuncOfSeq([ch \in active_children |-> [i \in 1..Len(range_seq[ch]) |->
                MakePktWithValue(RES, self, ch, range_seq[ch][i], BBuffer[Idx(range_seq[ch][i])])]]);
            RecircMultiple(pkts);
        }
    }
}

property {
    BEPsnInRange = \A nid \in SWITCH_SET :
        BEPsn[nid] >= BPsnStart[nid] /\ BEPsn[nid] <= BPsnStart[nid] + AGG_NUM;
    REPsnInRange = \A nid \in SWITCH_SET : \A ch \in ChildrenOf(nid) :
        REPsn[nid][ch] >= BPsnStart[nid] /\ REPsn[nid][ch] <= BPsnStart[nid] + AGG_NUM;
}
