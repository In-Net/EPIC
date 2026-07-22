---- MODULE aug_broadcast ----

EXTENDS Integers, Sequences, FiniteSets, TLC, Bitwise, FiniteSetsExt, SequencesExt, Functions

CONSTANTS
  TERMINATION_CHECK,
  MAX_LOSS,
  MAX_OUT_OF_ORDER,
  MAX_DUPLICATION,
  AGG_NUM,
  REQ_NUM,
  WINDOW_SIZE,
  FANIN,
  RES,
  ACK,
  NAK,
  ROOT,
  COMM_ROOT,
  r,
  s12,
  s34,
  c1,
  c2,
  c3,
  c4,
  null

(* --fair algorithm main {
  variables
    __net_buf = [__n \in NODE_SET |-> <<>>];
    __max_loss = MAX_LOSS;
    __max_out_of_order = MAX_OUT_OF_ORDER;
    __max_duplication = MAX_DUPLICATION;
    res = [i \in REQ_SET |-> RandomElement(0 .. 100000)];
    requests = [i \in REQ_SET |-> MakePktWithValue(RES, COMM_ROOT, __next_hop[COMM_ROOT, ROOT], i, res[i])];
    base = - 1;
    __active_threads = [__n \in SWITCH_SET |-> 3] @@ [__n \in CLIENT_SET |-> 3];
    BEPsn = [__n \in SWITCH_SET |-> (0)];
    BBuffer = [__n \in SWITCH_SET |-> ([i \in AGG_SET |-> 0])];
    BPsnStart = [__n \in SWITCH_SET |-> (0)];
    REPsn = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> 0])];
    nak_sent = [__n \in SWITCH_SET |-> (FALSE)];
    recirc_pkts = [__n \in SWITCH_SET |-> (<<>>)];
    res_epsn = [__n \in CLIENT_SET |-> (0)];

  define {
    SWITCH_SET == {r, s12, s34}
    SWITCH_NUM == Cardinality(SWITCH_SET)
    CLIENT_SET == {c1, c2, c3, c4}
    CLIENT_NUM == Cardinality(CLIENT_SET)
    NODE_SET == {r, s12, s34, c1, c2, c3, c4}
    NODE_NUM == Cardinality(NODE_SET)
    __links == r :> {s34, s12} @@ s12 :> {c2, c1, r} @@ s34 :> {c4, c3, r} @@ c1 :> {s12} @@ c2 :> {s12} @@ c3 :> {s34} @@ c4 :> {s34}
    __reliable_links == {<<c4, c4>>, <<c3, c3>>, <<c2, c2>>, <<c1, c1>>, <<s34, s34>>, <<s12, s12>>, <<r, r>>}
    __next_hop == 
          <<r, r>> :> r @@ <<r, s12>> :> s12 @@ <<r, s34>> :> s34
       @@ <<r, c1>> :> s12 @@ <<r, c2>> :> s12 @@ <<r, c3>> :> s34
       @@ <<r, c4>> :> s34 @@ <<s12, r>> :> r @@ <<s12, s12>> :> s12
       @@ <<s12, s34>> :> r @@ <<s12, c1>> :> c1 @@ <<s12, c2>> :> c2
       @@ <<s12, c3>> :> r @@ <<s12, c4>> :> r @@ <<s34, r>> :> r
       @@ <<s34, s12>> :> r @@ <<s34, s34>> :> s34 @@ <<s34, c1>> :> r
       @@ <<s34, c2>> :> r @@ <<s34, c3>> :> c3 @@ <<s34, c4>> :> c4
       @@ <<c1, r>> :> s12 @@ <<c1, s12>> :> s12 @@ <<c1, s34>> :> s12
       @@ <<c1, c1>> :> c1 @@ <<c1, c2>> :> s12 @@ <<c1, c3>> :> s12
       @@ <<c1, c4>> :> s12 @@ <<c2, r>> :> s12 @@ <<c2, s12>> :> s12
       @@ <<c2, s34>> :> s12 @@ <<c2, c1>> :> s12 @@ <<c2, c2>> :> c2
       @@ <<c2, c3>> :> s12 @@ <<c2, c4>> :> s12 @@ <<c3, r>> :> s34
       @@ <<c3, s12>> :> s34 @@ <<c3, s34>> :> s34 @@ <<c3, c1>> :> s34
       @@ <<c3, c2>> :> s34 @@ <<c3, c3>> :> c3 @@ <<c3, c4>> :> s34
       @@ <<c4, r>> :> s34 @@ <<c4, s12>> :> s34 @@ <<c4, s34>> :> s34
       @@ <<c4, c1>> :> s34 @@ <<c4, c2>> :> s34 @@ <<c4, c3>> :> s34
       @@ <<c4, c4>> :> c4
    REQ_SET == 0 .. (REQ_NUM - 1)
    AGG_SET == 0 .. (AGG_NUM - 1)

    __IsReliableLink(__src, __dst) == <<__src, __dst>> \in __reliable_links
    __IsUnreliableLink(__src, __dst) == <<__src, __dst>> \notin __reliable_links
    __MinPsnElem(__S) == CHOOSE __x \in __S : \A __y \in __S : __x.psn <= __y.psn
    __Set2OrderedSeq(__S) == 
      LET
        RECURSIVE F(_, _)
        F(__res, __SS) == IF __SS = {}
          THEN __res
          ELSE LET __x == __MinPsnElem(__SS) IN F(Append(__res, __x), __SS \ {__x})
      IN F(<<>>, __S)
    __OutOfOrderRange(__seq) == 1..Len(__seq)
    __InsertAtEnd(__seq, __i, __elem) == InsertAt(__seq, Len(__seq) - __i + 1, __elem)
    __Receive(__n) == Head(__net_buf[__n])
    __Node(__thread) == Head(__thread)
    __PosCount(__f) == Cardinality({__x \in DOMAIN __f : __f[__x] > 0})
    __AllPossibleLoss(__S) == {__s \in SUBSET __S : Cardinality(__s) <= __max_loss}
    __AllPossibleOutOfOrder(__S) == 
      LET
        __max_range == IF __S = {} THEN {} ELSE 0..Max({Len(__net_buf[__i]) : __i \in __S})
        __ooo_set == [__S -> __max_range]
        __ooo_set_possible == {__i \in __ooo_set : 
          /\ (\A __j \in DOMAIN __i : __i[__j] \in __OutOfOrderRange(__net_buf[__j]) \cup {0})
          /\ __PosCount(__i) <= __max_out_of_order
        }
      IN __ooo_set_possible
    __AllPossibleSeq(__S) == {__seq \in [1..Cardinality(__S) -> __S] : IsInjective(__seq)}
    __NodeTerminated(__n) == __active_threads[__n] <= 0
    SmallerOf(a, b) == IF a < b THEN a ELSE b
    LargerOf(a, b) == IF a > b THEN a ELSE b
    ParentOf(nid) == __next_hop[nid, COMM_ROOT]
    ChildrenOf(nid) == {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0}
    MakePkt(type, src, dst, psn) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn]
    MakePktWithValue(type, src, dst, psn, value) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn, value |-> value]
    Idx(psn) == psn % AGG_NUM
    IsCompleted(nid, epsn, psn) == \A ch \in ChildrenOf(nid) : epsn[ch] > psn
    MinEPsn(nid, epsn) == Min({epsn[ch] : ch \in ChildrenOf(nid)})
    SwitchCanTerminate(nid) == \A c \in NODE_SET : c > nid => __NodeTerminated(c)
    StrictRetxCondition(pkt) == \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>
    PsnSetToIdxSet(range) == {Idx(psn) : psn \in range}
    RangeToSeq(start, stop) == [i \in 1 .. (stop - start + 1) |-> i + start - 1]
    MergeFuncOfSeq(fos) == LET RECURSIVE F(_) F(fos_) == IF DOMAIN fos_ = {} THEN <<>> ELSE LET d == CHOOSE x \in DOMAIN fos_ : TRUE IN F([x \in DOMAIN fos_ \ {d} |-> fos_[x]]) \o fos_[d] IN F(fos)
  }


  macro __Assert(__cond, __msg) {
    with (__b = Assert(__cond, __msg)) {
      assert __b;
    }
  }

  macro __Drop() {
    __Assert(__net_buf[__Node(self)] # <<>>, "Drop: empty buffer");
    __net_buf[__Node(self)] := Tail(@);
  }

  macro __Send_NoDup(__pkt) {
    with (__h = __next_hop[__Node(self), __pkt.dst]) {
      either {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_out_of_order > 0;
        await __OutOfOrderRange(__net_buf[__h]) # {};
        with (__pos \in __OutOfOrderRange(__net_buf[__h])) {
          __net_buf[__h] := __InsertAtEnd(@, __pos, __pkt);
        };
        __max_out_of_order := __max_out_of_order - 1;
      }
      or {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_loss > 0;
        __max_loss := __max_loss - 1;
      }
      or {
        __net_buf[__h] := Append(@, __pkt);
      }
    }
  }

  macro __Send(__pkt) {
    with (
      has_dup = 
        /\ "id" \in DOMAIN __pkt
        /\ \E __n \in NODE_SET : \E __i \in DOMAIN __net_buf[__n] :
          "id" \in DOMAIN __net_buf[__n][__i] /\ __net_buf[__n][__i].id = __pkt.id
    ) {
      if (~has_dup) {
        __Send_NoDup(__pkt);
      }
      else {
        either {
          await __max_duplication > 0;
          __max_duplication := __max_duplication - 1;
          __Send_NoDup(__pkt);
        }
        or {
          skip;
        }
      }
    }
  }

  macro __DropSend(__pkt) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropSend: empty buffer");
    with (__s = __Node(self), __h = __next_hop[__s, __pkt.dst]) {
      either {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_out_of_order > 0;
        await __OutOfOrderRange(__net_buf[__h]) # {};
        with (__pos \in __OutOfOrderRange(__net_buf[__h])) {
          if (__s = __h) {
            __net_buf[__h] := Tail(__InsertAtEnd(@, __pos, __pkt));
          } else {
            __net_buf[__h] := __InsertAtEnd(@, __pos, __pkt) ||
            __net_buf[__s] := Tail(@);
          }
        };
        __max_out_of_order := __max_out_of_order - 1;
      }
      or {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_loss > 0;
        __max_loss := __max_loss - 1;
        __net_buf[__s] := Tail(@);
      }
      or {
        if (__s = __h) {
          __net_buf[__h] := Tail(Append(@, __pkt));
        } else {
          __net_buf[__h] := Append(@, __pkt) ||
          __net_buf[__s] := Tail(@);
        }
      }
    }
  }

  macro __Unicast(__pkts) {
    with (
      __keys = DOMAIN __pkts,
      __dst = __pkts[CHOOSE __i \in __keys : TRUE].dst,
      __h = __next_hop[__Node(self), __dst],
      __pkts_ordered = __Set2OrderedSeq({__pkts[__i] : __i \in __keys})
    ) {
      __Assert(Cardinality(__keys) > 0, "Unicast: empty packets");
      __Assert(
        \A __i \in __keys : __pkts[__i].dst = __dst,
        "Unicast: different destinations"
      );
      if (__IsReliableLink(__Node(self), __h)) {
        __net_buf[__h] := @ \o __pkts_ordered;
      }
      else {
        with (
          __loss \in __AllPossibleLoss(__keys),
          __unlost_pkts = {__pkts[__i] : __i \in __keys \ __loss},
          __ordered = __Set2OrderedSeq(__unlost_pkts)
        ) {
          __max_loss := __max_loss - Cardinality(__loss);
          either {
            await __max_out_of_order > 0;
            await Cardinality(__unlost_pkts) >= 2;
            with (__ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}) {
              __max_out_of_order := __max_out_of_order - 1;
              __net_buf[__h] := @ \o __ooo;
            }
          }
          or {
            __net_buf[__h] := @ \o __ordered;
          }
        }
      }
    }
  }

  macro __DropUnicast(__pkts) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropUnicast: empty buffer");
    with (
      __keys = DOMAIN __pkts,
      __dst = __pkts[CHOOSE __i \in __keys : TRUE].dst,
      __s = __Node(self),
      __h = __next_hop[__s, __dst],
      __pkts_ordered = __Set2OrderedSeq({__pkts[__i] : __i \in __keys})
    ) {
      __Assert(Cardinality(__keys) > 0, "DropUnicast: empty packets");
      __Assert(
        \A __i \in __keys : __pkts[__i].dst = __dst,
        "DropUnicast: different destinations"
      );
      if (__IsReliableLink(__s, __h)) {
        if (__s = __h) {
          __net_buf[__h] := Tail(@ \o __pkts_ordered);
        } else {
          __net_buf[__h] := @ \o __pkts_ordered ||
          __net_buf[__s] := Tail(@);
        }
      }
      else {
        with (
          __loss \in __AllPossibleLoss(__keys),
          __unlost_pkts = {__pkts[__i] : __i \in __keys \ __loss},
          __ordered = __Set2OrderedSeq(__unlost_pkts)
        ) {
          __max_loss := __max_loss - Cardinality(__loss);
          either {
            await __max_out_of_order > 0;
            await Cardinality(__unlost_pkts) >= 2;
            with (__ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}) {
              __max_out_of_order := __max_out_of_order - 1;
              if (__s = __h) {
                __net_buf[__h] := Tail(@ \o __ooo);
              } else {
                __net_buf[__h] := @ \o __ooo ||
                __net_buf[__s] := Tail(@);
              }
            }
          }
          or {
            if (__s = __h) {
              __net_buf[__h] := Tail(@ \o __ordered);
            } else {
              __net_buf[__h] := @ \o __ordered ||
              __net_buf[__s] := Tail(@);
            }
          }
        }
      }
    }
  }

  macro __Multicast(__pkt, __dsts) {
    __Assert(__dsts \subseteq __links[__Node(self)], "Multicast: invalid destinations");
    __Assert(__Node(self) \notin __dsts, "DropMulticast: self in destinations");
    with (
      __pkts = [__dst \in __dsts |-> "dst" :> __dst @@ __pkt],
      __possible_losses_1 = __AllPossibleLoss(__dsts),
      __possible_losses_2 = {__l \in __possible_losses_1 :
        \A __d \in __l : __IsUnreliableLink(__Node(self), __d)},
      __loss \in __possible_losses_2,
      __unlost_dsts = __dsts \ __loss,
      __possible_ooo_1 = __AllPossibleOutOfOrder(__unlost_dsts),
      __possible_ooo_2 = {__o \in __possible_ooo_1 : \A __d \in DOMAIN __o : __o[__d] > 0 => __IsUnreliableLink(__Node(self), __d)},
      __ooo \in __possible_ooo_2
    ) {
      __net_buf := [__n \in NODE_SET |->
        CASE __n \in __unlost_dsts -> __InsertAtEnd(__net_buf[__n], __ooo[__n], __pkts[__n])
        [] OTHER -> __net_buf[__n]
      ];
      __max_loss := __max_loss - Cardinality(__loss);
      __max_out_of_order := __max_out_of_order - __PosCount(__ooo);
    }
  }

  macro __DropMulticast(__pkt, __dsts) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropMulticast: empty buffer");
    __Assert(__dsts \subseteq __links[__Node(self)], "DropMulticast: invalid destinations");
    __Assert(__Node(self) \notin __dsts, "DropMulticast: self in destinations");
    with (
      __pkts = [__dst \in __dsts |-> "dst" :> __dst @@ __pkt],
      __possible_losses_1 = __AllPossibleLoss(__dsts),
      __possible_losses_2 = {__l \in __possible_losses_1 :
        \A __d \in __l : __IsUnreliableLink(__Node(self), __d)},
      __loss \in __possible_losses_2,
      __unlost_dsts = __dsts \ __loss,
      __possible_ooo_1 = __AllPossibleOutOfOrder(__unlost_dsts),
      __possible_ooo_2 = {__o \in __possible_ooo_1 :
        \A __d \in DOMAIN __o : __o[__d] > 0 => __IsUnreliableLink(__Node(self), __d)},
      __ooo \in __possible_ooo_2
    ) {
      __net_buf := [__n \in NODE_SET |->
        CASE __n \in __unlost_dsts -> __InsertAtEnd(__net_buf[__n], __ooo[__n], __pkts[__n])
        [] __n = __Node(self) -> Tail(__net_buf[__n])
        [] OTHER -> __net_buf[__n]
      ];
      __max_loss := __max_loss - Cardinality(__loss);
      __max_out_of_order := __max_out_of_order - __PosCount(__ooo);
    }
  }

  macro __Wait(__cond) {
    await __cond;
  }

  macro __Exit() {
    __active_threads[__Node(self)] := 0;
    goto Done;
  }

  macro __Print(__x) {
    print __x;
  }

  macro __CheckCacheConsistency(__pkt, __is_start) {
    skip;
  }


  fair+ process (ClientInitSend \in (CLIENT_SET \X {"ClientInitSend"})) {
  L_ClientInitSend:
    if (__active_threads[__Node(self)] <= 0) { goto Done; };
    else {
      if (__Node(self) = COMM_ROOT) {
        with (
          end_idx = SmallerOf(WINDOW_SIZE, REQ_NUM),
        ) {
          __Unicast([i \in 0 .. (end_idx - 1) |-> requests[i]]);
          base := 0;
        }
      };
    };
\*   __L_ClientInitSend_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (ClientRecv \in (CLIENT_SET \X {"ClientRecv"})) {
  L_ClientRecv:
    while(TRUE) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          pkt = __Receive(__Node(self)),
        ) {
          __Wait(__net_buf[__Node(self)] # <<>>);
          if (pkt.type = ACK) {
            __Assert(__Node(self) = COMM_ROOT, "ClientAck: non-root client receives ACK.");
            if ((pkt).psn > base) {
              with (
                start_idx = base + WINDOW_SIZE,
                end_idx = SmallerOf((pkt).psn + WINDOW_SIZE, REQ_NUM),
              ) {
                if (start_idx < end_idx) {
                  __DropUnicast([i \in start_idx .. (end_idx - 1) |-> requests[i]]);
                };
                else {
                  __Drop();
                };
                base := (pkt).psn;
                if (base = REQ_NUM) {
                  __Exit();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else if (pkt.type = NAK) {
            __Assert(__Node(self) = COMM_ROOT, "ClientNak: non-root client receives NAK.");
            if ((pkt).psn >= base) {
              with (
                next = __next_hop[__Node(self), (requests[(pkt).psn]).dst],
              ) {
                if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (requests[(pkt).psn]))) {
                  __DropSend((requests[(pkt).psn]));
                };
                else {
                  __Drop();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else if (pkt.type = RES) {
            __Assert(__Node(self) # COMM_ROOT, "ClientRes: root client receives RES.");
            __Assert((pkt).value = res[(pkt).psn], "ClientRes: incorrect result.");
            if ((pkt).psn = res_epsn[__Node(self)]) {
              with (
                next = __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn[__Node(self)]))).dst],
              ) {
                res_epsn[__Node(self)] := res_epsn[__Node(self)] + 1;
                if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn[__Node(self)]))))) {
                  __DropSend((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn[__Node(self)]))));
                };
                else {
                  __Drop();
                };
                if (res_epsn[__Node(self)] = REQ_NUM) {
                  __Exit();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else {
            __Assert(FALSE, "ClientRecv: unexpected packet type");
            __Drop();
          };
        }
      };
    };
\*   __L_ClientRecv_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (ClientRetx \in (CLIENT_SET \X {"ClientRetx"})) {
  L_ClientRetx:
    while(__Node(self) = COMM_ROOT /\ base < REQ_NUM) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          end_idx = SmallerOf(base + WINDOW_SIZE, REQ_NUM),
        ) {
          __Wait(__net_buf[__Node(self)] = <<>>);
          __Wait(base >= 0);
          __Wait(StrictRetxCondition(0));
          __Unicast([i \in base .. (end_idx - 1) |-> requests[i]]);
        }
      };
    };
\*   __L_ClientRetx_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (SwitchRecv \in (SWITCH_SET \X {"SwitchRecv"})) {
  L_SwitchRecv:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          pkt = __Receive(__Node(self)),
        ) {
          __Wait(recirc_pkts[__Node(self)] = <<>>);
          __Wait(__net_buf[__Node(self)] # <<>>);
          if (pkt.type = RES) {
            with (
              idx = Idx((pkt).psn),
            ) {
              __Assert((pkt).src = ParentOf(__Node(self)), "SwitchRes: switch receives RES from non-parent.");
              if ((pkt).psn < BEPsn[__Node(self)]) {
                with (
                  next = __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))).dst],
                ) {
                  if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))) {
                    __DropSend((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))));
                  };
                  else {
                    __Drop();
                  };
                }
              };
              else if ((pkt).psn > BEPsn[__Node(self)]) {
                if (~nak_sent[__Node(self)]) {
                  with (
                    next = __next_hop[__Node(self), (MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))).dst],
                  ) {
                    if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))) {
                      __DropSend((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))));
                    };
                    else {
                      __Drop();
                    };
                    nak_sent[__Node(self)] := TRUE;
                  }
                };
                else {
                  __Drop();
                };
              };
              else {
                nak_sent[__Node(self)] := FALSE;
                if ((pkt).psn >= BPsnStart[__Node(self)] + AGG_NUM) {
                  skip;
                  __Drop();
                };
                else {
                  BBuffer[__Node(self)][idx] := (pkt).value;
                  BEPsn[__Node(self)] := BEPsn[__Node(self)] + 1;
                  __DropMulticast(MakePktWithValue(RES, __Node(self), __Node(self), (pkt).psn, (pkt).value), ChildrenOf(__Node(self)));
                  recirc_pkts[__Node(self)] := Append(recirc_pkts[__Node(self)], (MakePkt(ACK, __Node(self), (pkt).src, BEPsn[__Node(self)])));
                };
              };
            }
          };
          else if (pkt.type = ACK) {
            with (
              idx = Idx((pkt).psn),
              ch = (pkt).src,
            ) {
              __Assert(ch \in ChildrenOf(__Node(self)), "SwitchAck: switch receives ACK from non-child.");
              __Assert(BEPsn[__Node(self)] >= (pkt).psn, "SwitchAck: switch receives ACK for unreceived result.");
              if ((pkt).psn > REPsn[__Node(self)][ch]) {
                with (
                  new_start = MinEPsn(__Node(self), ch :> (pkt).psn @@ REPsn[__Node(self)]),
                  range = BPsnStart[__Node(self)] .. (new_start - 1),
                ) {
                  REPsn[__Node(self)][ch] := (pkt).psn;
                  if (new_start > BPsnStart[__Node(self)]) {
                    BBuffer[__Node(self)] := [i \in PsnSetToIdxSet(range) |-> 0] @@ BBuffer[__Node(self)];
                    BPsnStart[__Node(self)] := new_start;
                  };
                }
              };
              __Drop();
            }
          };
          else if (pkt.type = NAK) {
            with (
              idx = Idx((pkt).psn),
              ch = (pkt).src,
              max_psn = BEPsn[__Node(self)] - 1,
              range_seq = RangeToSeq(REPsn[__Node(self)][ch], max_psn),
              pkts = [i \in 1 .. Len(range_seq) |-> MakePktWithValue(RES, __Node(self), ch, range_seq[i], BBuffer[__Node(self)][Idx(range_seq[i])])],
            ) {
              __Assert(ch \in ChildrenOf(__Node(self)), "SwitchNak: switch receives NAK from non-child.");
              __Assert((pkt).psn = REPsn[__Node(self)][ch], "SwitchNak: we currently do not consider out-of-order networks.");
              recirc_pkts[__Node(self)] := recirc_pkts[__Node(self)] \o (pkts);
              __Drop();
            }
          };
          else {
            __Assert(FALSE, "SwitchRecv: unexpected packet type");
            __Drop();
          };
        }
      };
    };
\*   __L_SwitchRecv_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
    __Exit();
  }

  fair+ process (SwitchRecirc \in (SWITCH_SET \X {"SwitchRecirc"})) {
  L_SwitchRecirc:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          next = __next_hop[__Node(self), (Head(recirc_pkts[__Node(self)])).dst],
        ) {
          __Wait(recirc_pkts[__Node(self)] # <<>>);
          if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (Head(recirc_pkts[__Node(self)])))) {
            __Send((Head(recirc_pkts[__Node(self)])));
          };
          recirc_pkts[__Node(self)] := Tail(recirc_pkts[__Node(self)]);
        }
      };
    };
\*   __L_SwitchRecirc_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (SwitchRetxRes \in (SWITCH_SET \X {"SwitchRetxRes"})) {
  L_SwitchRetxRes:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          max_psn = BEPsn[__Node(self)] - 1,
          active_children = {ch \in ChildrenOf(__Node(self)) : ~__NodeTerminated(ch)},
          range_seq = [ch \in active_children |-> RangeToSeq(REPsn[__Node(self)][ch], max_psn)],
          pkts = MergeFuncOfSeq([ch \in active_children |-> [i \in 1 .. Len(range_seq[ch]) |-> MakePktWithValue(RES, __Node(self), ch, range_seq[ch][i], BBuffer[__Node(self)][Idx(range_seq[ch][i])])]]),
        ) {
          __Wait(recirc_pkts[__Node(self)] = <<>>);
          __Wait(StrictRetxCondition(0));
          recirc_pkts[__Node(self)] := recirc_pkts[__Node(self)] \o (pkts);
        }
      };
    };
\*   __L_SwitchRetxRes_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }
} *)
\* BEGIN TRANSLATION (chksum(pcal) = "7c28162a" /\ chksum(tla) = "cc37febc")
VARIABLES pc, __net_buf, __max_loss, __max_out_of_order, __max_duplication, 
          res, requests, base, __active_threads, BEPsn, BBuffer, BPsnStart, 
          REPsn, nak_sent, recirc_pkts, res_epsn

(* define statement *)
SWITCH_SET == {r, s12, s34}
SWITCH_NUM == Cardinality(SWITCH_SET)
CLIENT_SET == {c1, c2, c3, c4}
CLIENT_NUM == Cardinality(CLIENT_SET)
NODE_SET == {r, s12, s34, c1, c2, c3, c4}
NODE_NUM == Cardinality(NODE_SET)
__links == r :> {s34, s12} @@ s12 :> {c2, c1, r} @@ s34 :> {c4, c3, r} @@ c1 :> {s12} @@ c2 :> {s12} @@ c3 :> {s34} @@ c4 :> {s34}
__reliable_links == {<<c4, c4>>, <<c3, c3>>, <<c2, c2>>, <<c1, c1>>, <<s34, s34>>, <<s12, s12>>, <<r, r>>}
__next_hop ==
      <<r, r>> :> r @@ <<r, s12>> :> s12 @@ <<r, s34>> :> s34
   @@ <<r, c1>> :> s12 @@ <<r, c2>> :> s12 @@ <<r, c3>> :> s34
   @@ <<r, c4>> :> s34 @@ <<s12, r>> :> r @@ <<s12, s12>> :> s12
   @@ <<s12, s34>> :> r @@ <<s12, c1>> :> c1 @@ <<s12, c2>> :> c2
   @@ <<s12, c3>> :> r @@ <<s12, c4>> :> r @@ <<s34, r>> :> r
   @@ <<s34, s12>> :> r @@ <<s34, s34>> :> s34 @@ <<s34, c1>> :> r
   @@ <<s34, c2>> :> r @@ <<s34, c3>> :> c3 @@ <<s34, c4>> :> c4
   @@ <<c1, r>> :> s12 @@ <<c1, s12>> :> s12 @@ <<c1, s34>> :> s12
   @@ <<c1, c1>> :> c1 @@ <<c1, c2>> :> s12 @@ <<c1, c3>> :> s12
   @@ <<c1, c4>> :> s12 @@ <<c2, r>> :> s12 @@ <<c2, s12>> :> s12
   @@ <<c2, s34>> :> s12 @@ <<c2, c1>> :> s12 @@ <<c2, c2>> :> c2
   @@ <<c2, c3>> :> s12 @@ <<c2, c4>> :> s12 @@ <<c3, r>> :> s34
   @@ <<c3, s12>> :> s34 @@ <<c3, s34>> :> s34 @@ <<c3, c1>> :> s34
   @@ <<c3, c2>> :> s34 @@ <<c3, c3>> :> c3 @@ <<c3, c4>> :> s34
   @@ <<c4, r>> :> s34 @@ <<c4, s12>> :> s34 @@ <<c4, s34>> :> s34
   @@ <<c4, c1>> :> s34 @@ <<c4, c2>> :> s34 @@ <<c4, c3>> :> s34
   @@ <<c4, c4>> :> c4
REQ_SET == 0 .. (REQ_NUM - 1)
AGG_SET == 0 .. (AGG_NUM - 1)

__IsReliableLink(__src, __dst) == <<__src, __dst>> \in __reliable_links
__IsUnreliableLink(__src, __dst) == <<__src, __dst>> \notin __reliable_links
__MinPsnElem(__S) == CHOOSE __x \in __S : \A __y \in __S : __x.psn <= __y.psn
__Set2OrderedSeq(__S) ==
  LET
    RECURSIVE F(_, _)
    F(__res, __SS) == IF __SS = {}
      THEN __res
      ELSE LET __x == __MinPsnElem(__SS) IN F(Append(__res, __x), __SS \ {__x})
  IN F(<<>>, __S)
__OutOfOrderRange(__seq) == 1..Len(__seq)
__InsertAtEnd(__seq, __i, __elem) == InsertAt(__seq, Len(__seq) - __i + 1, __elem)
__Receive(__n) == Head(__net_buf[__n])
__Node(__thread) == Head(__thread)
__PosCount(__f) == Cardinality({__x \in DOMAIN __f : __f[__x] > 0})
__AllPossibleLoss(__S) == {__s \in SUBSET __S : Cardinality(__s) <= __max_loss}
__AllPossibleOutOfOrder(__S) ==
  LET
    __max_range == IF __S = {} THEN {} ELSE 0..Max({Len(__net_buf[__i]) : __i \in __S})
    __ooo_set == [__S -> __max_range]
    __ooo_set_possible == {__i \in __ooo_set :
      /\ (\A __j \in DOMAIN __i : __i[__j] \in __OutOfOrderRange(__net_buf[__j]) \cup {0})
      /\ __PosCount(__i) <= __max_out_of_order
    }
  IN __ooo_set_possible
__AllPossibleSeq(__S) == {__seq \in [1..Cardinality(__S) -> __S] : IsInjective(__seq)}
__NodeTerminated(__n) == __active_threads[__n] <= 0
SmallerOf(a, b) == IF a < b THEN a ELSE b
LargerOf(a, b) == IF a > b THEN a ELSE b
ParentOf(nid) == __next_hop[nid, COMM_ROOT]
ChildrenOf(nid) == {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0}
MakePkt(type, src, dst, psn) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn]
MakePktWithValue(type, src, dst, psn, value) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn, value |-> value]
Idx(psn) == psn % AGG_NUM
IsCompleted(nid, epsn, psn) == \A ch \in ChildrenOf(nid) : epsn[ch] > psn
MinEPsn(nid, epsn) == Min({epsn[ch] : ch \in ChildrenOf(nid)})
SwitchCanTerminate(nid) == \A c \in NODE_SET : c > nid => __NodeTerminated(c)
StrictRetxCondition(pkt) == \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>
PsnSetToIdxSet(range) == {Idx(psn) : psn \in range}
RangeToSeq(start, stop) == [i \in 1 .. (stop - start + 1) |-> i + start - 1]
MergeFuncOfSeq(fos) == LET RECURSIVE F(_) F(fos_) == IF DOMAIN fos_ = {} THEN <<>> ELSE LET d == CHOOSE x \in DOMAIN fos_ : TRUE IN F([x \in DOMAIN fos_ \ {d} |-> fos_[x]]) \o fos_[d] IN F(fos)


vars == << pc, __net_buf, __max_loss, __max_out_of_order, __max_duplication, 
           res, requests, base, __active_threads, BEPsn, BBuffer, BPsnStart, 
           REPsn, nak_sent, recirc_pkts, res_epsn >>

ProcSet == ((CLIENT_SET \X {"ClientInitSend"})) \cup ((CLIENT_SET \X {"ClientRecv"})) \cup ((CLIENT_SET \X {"ClientRetx"})) \cup ((SWITCH_SET \X {"SwitchRecv"})) \cup ((SWITCH_SET \X {"SwitchRecirc"})) \cup ((SWITCH_SET \X {"SwitchRetxRes"}))

Init == (* Global variables *)
        /\ __net_buf = [__n \in NODE_SET |-> <<>>]
        /\ __max_loss = MAX_LOSS
        /\ __max_out_of_order = MAX_OUT_OF_ORDER
        /\ __max_duplication = MAX_DUPLICATION
        /\ res = [i \in REQ_SET |-> RandomElement(0 .. 100000)]
        /\ requests = [i \in REQ_SET |-> MakePktWithValue(RES, COMM_ROOT, __next_hop[COMM_ROOT, ROOT], i, res[i])]
        /\ base = - 1
        /\ __active_threads = [__n \in SWITCH_SET |-> 3] @@ [__n \in CLIENT_SET |-> 3]
        /\ BEPsn = [__n \in SWITCH_SET |-> (0)]
        /\ BBuffer = [__n \in SWITCH_SET |-> ([i \in AGG_SET |-> 0])]
        /\ BPsnStart = [__n \in SWITCH_SET |-> (0)]
        /\ REPsn = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> 0])]
        /\ nak_sent = [__n \in SWITCH_SET |-> (FALSE)]
        /\ recirc_pkts = [__n \in SWITCH_SET |-> (<<>>)]
        /\ res_epsn = [__n \in CLIENT_SET |-> (0)]
        /\ pc = [self \in ProcSet |-> CASE self \in (CLIENT_SET \X {"ClientInitSend"}) -> "L_ClientInitSend"
                                        [] self \in (CLIENT_SET \X {"ClientRecv"}) -> "L_ClientRecv"
                                        [] self \in (CLIENT_SET \X {"ClientRetx"}) -> "L_ClientRetx"
                                        [] self \in (SWITCH_SET \X {"SwitchRecv"}) -> "L_SwitchRecv"
                                        [] self \in (SWITCH_SET \X {"SwitchRecirc"}) -> "L_SwitchRecirc"
                                        [] self \in (SWITCH_SET \X {"SwitchRetxRes"}) -> "L_SwitchRetxRes"]

L_ClientInitSend(self) == /\ pc[self] = "L_ClientInitSend"
                          /\ IF __active_threads[__Node(self)] <= 0
                                THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                     /\ UNCHANGED << __net_buf, __max_loss, 
                                                     __max_out_of_order, base >>
                                ELSE /\ IF __Node(self) = COMM_ROOT
                                           THEN /\ LET end_idx == SmallerOf(WINDOW_SIZE, REQ_NUM) IN
                                                     /\ LET __keys == DOMAIN ([i \in 0 .. (end_idx - 1) |-> requests[i]]) IN
                                                          LET __dst == ([i \in 0 .. (end_idx - 1) |-> requests[i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                            LET __h == __next_hop[__Node(self), __dst] IN
                                                              LET __pkts_ordered == __Set2OrderedSeq({([i \in 0 .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys}) IN
                                                                /\ LET __b == Assert((Cardinality(__keys) > 0), "Unicast: empty packets") IN
                                                                     Assert(__b, 
                                                                            "Failure of assertion at line 122, column 7 of macro called at line 378, column 11.")
                                                                /\ LET __b == Assert((\A __i \in __keys : ([i \in 0 .. (end_idx - 1) |-> requests[i]])[__i].dst = __dst), "Unicast: different destinations") IN
                                                                     Assert(__b, 
                                                                            "Failure of assertion at line 122, column 7 of macro called at line 378, column 11.")
                                                                /\ IF __IsReliableLink(__Node(self), __h)
                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered]
                                                                           /\ UNCHANGED << __max_loss, 
                                                                                           __max_out_of_order >>
                                                                      ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                                LET __unlost_pkts == {([i \in 0 .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys \ __loss} IN
                                                                                  LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                    /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                    /\ \/ /\ __max_out_of_order > 0
                                                                                          /\ Cardinality(__unlost_pkts) >= 2
                                                                                          /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                               /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                               /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo]
                                                                                       \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered]
                                                                                          /\ UNCHANGED __max_out_of_order
                                                     /\ base' = 0
                                           ELSE /\ TRUE
                                                /\ UNCHANGED << __net_buf, 
                                                                __max_loss, 
                                                                __max_out_of_order, 
                                                                base >>
                                     /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << __max_duplication, res, requests, 
                                          __active_threads, BEPsn, BBuffer, 
                                          BPsnStart, REPsn, nak_sent, 
                                          recirc_pkts, res_epsn >>

ClientInitSend(self) == L_ClientInitSend(self)

L_ClientRecv(self) == /\ pc[self] = "L_ClientRecv"
                      /\ IF __active_threads[__Node(self)] <= 0
                            THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order, base, 
                                                 __active_threads, res_epsn >>
                            ELSE /\ LET pkt == __Receive(__Node(self)) IN
                                      /\ __net_buf[__Node(self)] # <<>>
                                      /\ IF pkt.type = ACK
                                            THEN /\ LET __b == Assert((__Node(self) = COMM_ROOT), "ClientAck: non-root client receives ACK.") IN
                                                      Assert(__b, 
                                                             "Failure of assertion at line 122, column 7 of macro called at line 399, column 13.")
                                                 /\ IF (pkt).psn > base
                                                       THEN /\ LET start_idx == base + WINDOW_SIZE IN
                                                                 LET end_idx == SmallerOf((pkt).psn + WINDOW_SIZE, REQ_NUM) IN
                                                                   /\ IF start_idx < end_idx
                                                                         THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropUnicast: empty buffer") IN
                                                                                   Assert(__b, 
                                                                                          "Failure of assertion at line 122, column 7 of macro called at line 406, column 19.")
                                                                              /\ LET __keys == DOMAIN ([i \in start_idx .. (end_idx - 1) |-> requests[i]]) IN
                                                                                   LET __dst == ([i \in start_idx .. (end_idx - 1) |-> requests[i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                                                     LET __s == __Node(self) IN
                                                                                       LET __h == __next_hop[__s, __dst] IN
                                                                                         LET __pkts_ordered == __Set2OrderedSeq({([i \in start_idx .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys}) IN
                                                                                           /\ LET __b == Assert((Cardinality(__keys) > 0), "DropUnicast: empty packets") IN
                                                                                                Assert(__b, 
                                                                                                       "Failure of assertion at line 122, column 7 of macro called at line 406, column 19.")
                                                                                           /\ LET __b == Assert((\A __i \in __keys : ([i \in start_idx .. (end_idx - 1) |-> requests[i]])[__i].dst = __dst), "DropUnicast: different destinations") IN
                                                                                                Assert(__b, 
                                                                                                       "Failure of assertion at line 122, column 7 of macro called at line 406, column 19.")
                                                                                           /\ IF __IsReliableLink(__s, __h)
                                                                                                 THEN /\ IF __s = __h
                                                                                                            THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __pkts_ordered)]
                                                                                                            ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered,
                                                                                                                                                   ![__s] = Tail(@)]
                                                                                                      /\ UNCHANGED << __max_loss, 
                                                                                                                      __max_out_of_order >>
                                                                                                 ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                                                           LET __unlost_pkts == {([i \in start_idx .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys \ __loss} IN
                                                                                                             LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                                               /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                                               /\ \/ /\ __max_out_of_order > 0
                                                                                                                     /\ Cardinality(__unlost_pkts) >= 2
                                                                                                                     /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                                                          /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                                          /\ IF __s = __h
                                                                                                                                THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __ooo)]
                                                                                                                                ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo,
                                                                                                                                                                       ![__s] = Tail(@)]
                                                                                                                  \/ /\ IF __s = __h
                                                                                                                           THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __ordered)]
                                                                                                                           ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered,
                                                                                                                                                                  ![__s] = Tail(@)]
                                                                                                                     /\ UNCHANGED __max_out_of_order
                                                                         ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                   Assert(__b, 
                                                                                          "Failure of assertion at line 122, column 7 of macro called at line 409, column 19.")
                                                                              /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                              /\ UNCHANGED << __max_loss, 
                                                                                              __max_out_of_order >>
                                                                   /\ base' = (pkt).psn
                                                                   /\ IF base' = REQ_NUM
                                                                         THEN /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                                                              /\ pc' = [pc EXCEPT ![self] = "Done"]
                                                                         ELSE /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                              /\ UNCHANGED __active_threads
                                                       ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 122, column 7 of macro called at line 418, column 15.")
                                                            /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                            /\ UNCHANGED << __max_loss, 
                                                                            __max_out_of_order, 
                                                                            base, 
                                                                            __active_threads >>
                                                 /\ UNCHANGED res_epsn
                                            ELSE /\ IF pkt.type = NAK
                                                       THEN /\ LET __b == Assert((__Node(self) = COMM_ROOT), "ClientNak: non-root client receives NAK.") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 122, column 7 of macro called at line 422, column 13.")
                                                            /\ IF (pkt).psn >= base
                                                                  THEN /\ LET next == __next_hop[__Node(self), (requests[(pkt).psn]).dst] IN
                                                                            IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (requests[(pkt).psn]))
                                                                               THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                         Assert(__b, 
                                                                                                "Failure of assertion at line 122, column 7 of macro called at line 428, column 19.")
                                                                                    /\ LET __s == __Node(self) IN
                                                                                         LET __h == __next_hop[__s, ((requests[(pkt).psn])).dst] IN
                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                              /\ __max_out_of_order > 0
                                                                                              /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                              /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                   IF __s = __h
                                                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((requests[(pkt).psn]))))]
                                                                                                      ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((requests[(pkt).psn]))),
                                                                                                                                             ![__s] = Tail(@)]
                                                                                              /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                              /\ UNCHANGED __max_loss
                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                              /\ __max_loss > 0
                                                                                              /\ __max_loss' = __max_loss - 1
                                                                                              /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                              /\ UNCHANGED __max_out_of_order
                                                                                           \/ /\ IF __s = __h
                                                                                                    THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((requests[(pkt).psn]))))]
                                                                                                    ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((requests[(pkt).psn]))),
                                                                                                                                           ![__s] = Tail(@)]
                                                                                              /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                               ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                         Assert(__b, 
                                                                                                "Failure of assertion at line 122, column 7 of macro called at line 431, column 19.")
                                                                                    /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                    /\ UNCHANGED << __max_loss, 
                                                                                                    __max_out_of_order >>
                                                                  ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 122, column 7 of macro called at line 436, column 15.")
                                                                       /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order >>
                                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                            /\ UNCHANGED << __active_threads, 
                                                                            res_epsn >>
                                                       ELSE /\ IF pkt.type = RES
                                                                  THEN /\ LET __b == Assert((__Node(self) # COMM_ROOT), "ClientRes: root client receives RES.") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 122, column 7 of macro called at line 440, column 13.")
                                                                       /\ LET __b == Assert(((pkt).value = res[(pkt).psn]), "ClientRes: incorrect result.") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 122, column 7 of macro called at line 441, column 13.")
                                                                       /\ IF (pkt).psn = res_epsn[__Node(self)]
                                                                             THEN /\ LET next == __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn[__Node(self)]))).dst] IN
                                                                                       /\ res_epsn' = [res_epsn EXCEPT ![__Node(self)] = res_epsn[__Node(self)] + 1]
                                                                                       /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)]))))
                                                                                             THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 448, column 19.")
                                                                                                  /\ LET __s == __Node(self) IN
                                                                                                       LET __h == __next_hop[__s, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)])))).dst] IN
                                                                                                         \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                            /\ __max_out_of_order > 0
                                                                                                            /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                            /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                                 IF __s = __h
                                                                                                                    THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)]))))))]
                                                                                                                    ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)]))))),
                                                                                                                                                           ![__s] = Tail(@)]
                                                                                                            /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                            /\ UNCHANGED __max_loss
                                                                                                         \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                            /\ __max_loss > 0
                                                                                                            /\ __max_loss' = __max_loss - 1
                                                                                                            /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                            /\ UNCHANGED __max_out_of_order
                                                                                                         \/ /\ IF __s = __h
                                                                                                                  THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)]))))))]
                                                                                                                  ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'[__Node(self)]))))),
                                                                                                                                                         ![__s] = Tail(@)]
                                                                                                            /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                             ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 451, column 19.")
                                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                                  __max_out_of_order >>
                                                                                       /\ IF res_epsn'[__Node(self)] = REQ_NUM
                                                                                             THEN /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                                                                                  /\ pc' = [pc EXCEPT ![self] = "Done"]
                                                                                             ELSE /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                                                  /\ UNCHANGED __active_threads
                                                                             ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 459, column 15.")
                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                  /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                  __max_out_of_order, 
                                                                                                  __active_threads, 
                                                                                                  res_epsn >>
                                                                  ELSE /\ LET __b == Assert(FALSE, "ClientRecv: unexpected packet type") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 122, column 7 of macro called at line 463, column 13.")
                                                                       /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 122, column 7 of macro called at line 464, column 13.")
                                                                       /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order, 
                                                                                       __active_threads, 
                                                                                       res_epsn >>
                                                 /\ base' = base
                      /\ UNCHANGED << __max_duplication, res, requests, BEPsn, 
                                      BBuffer, BPsnStart, REPsn, nak_sent, 
                                      recirc_pkts >>

ClientRecv(self) == L_ClientRecv(self)

L_ClientRetx(self) == /\ pc[self] = "L_ClientRetx"
                      /\ IF __Node(self) = COMM_ROOT /\ base < REQ_NUM
                            THEN /\ IF __active_threads[__Node(self)] <= 0
                                       THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                            /\ UNCHANGED << __net_buf, 
                                                            __max_loss, 
                                                            __max_out_of_order >>
                                       ELSE /\ LET end_idx == SmallerOf(base + WINDOW_SIZE, REQ_NUM) IN
                                                 /\ __net_buf[__Node(self)] = <<>>
                                                 /\ base >= 0
                                                 /\ StrictRetxCondition(0)
                                                 /\ LET __keys == DOMAIN ([i \in base .. (end_idx - 1) |-> requests[i]]) IN
                                                      LET __dst == ([i \in base .. (end_idx - 1) |-> requests[i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                        LET __h == __next_hop[__Node(self), __dst] IN
                                                          LET __pkts_ordered == __Set2OrderedSeq({([i \in base .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys}) IN
                                                            /\ LET __b == Assert((Cardinality(__keys) > 0), "Unicast: empty packets") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 122, column 7 of macro called at line 486, column 11.")
                                                            /\ LET __b == Assert((\A __i \in __keys : ([i \in base .. (end_idx - 1) |-> requests[i]])[__i].dst = __dst), "Unicast: different destinations") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 122, column 7 of macro called at line 486, column 11.")
                                                            /\ IF __IsReliableLink(__Node(self), __h)
                                                                  THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order >>
                                                                  ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                            LET __unlost_pkts == {([i \in base .. (end_idx - 1) |-> requests[i]])[__i] : __i \in __keys \ __loss} IN
                                                                              LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                /\ \/ /\ __max_out_of_order > 0
                                                                                      /\ Cardinality(__unlost_pkts) >= 2
                                                                                      /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                           /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                           /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo]
                                                                                   \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered]
                                                                                      /\ UNCHANGED __max_out_of_order
                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRetx"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order >>
                      /\ UNCHANGED << __max_duplication, res, requests, base, 
                                      __active_threads, BEPsn, BBuffer, 
                                      BPsnStart, REPsn, nak_sent, recirc_pkts, 
                                      res_epsn >>

ClientRetx(self) == L_ClientRetx(self)

L_SwitchRecv(self) == /\ pc[self] = "L_SwitchRecv"
                      /\ IF ~SwitchCanTerminate(__Node(self))
                            THEN /\ IF __active_threads[__Node(self)] <= 0
                                       THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                            /\ UNCHANGED << __net_buf, 
                                                            __max_loss, 
                                                            __max_out_of_order, 
                                                            BEPsn, BBuffer, 
                                                            BPsnStart, REPsn, 
                                                            nak_sent, 
                                                            recirc_pkts >>
                                       ELSE /\ LET pkt == __Receive(__Node(self)) IN
                                                 /\ recirc_pkts[__Node(self)] = <<>>
                                                 /\ __net_buf[__Node(self)] # <<>>
                                                 /\ IF pkt.type = RES
                                                       THEN /\ LET idx == Idx((pkt).psn) IN
                                                                 /\ LET __b == Assert(((pkt).src = ParentOf(__Node(self))), "SwitchRes: switch receives RES from non-parent.") IN
                                                                      Assert(__b, 
                                                                             "Failure of assertion at line 122, column 7 of macro called at line 510, column 15.")
                                                                 /\ IF (pkt).psn < BEPsn[__Node(self)]
                                                                       THEN /\ LET next == __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))).dst] IN
                                                                                 IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))
                                                                                    THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                              Assert(__b, 
                                                                                                     "Failure of assertion at line 122, column 7 of macro called at line 516, column 21.")
                                                                                         /\ LET __s == __Node(self) IN
                                                                                              LET __h == __next_hop[__s, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)])))).dst] IN
                                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                   /\ __max_out_of_order > 0
                                                                                                   /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                   /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                        IF __s = __h
                                                                                                           THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))))]
                                                                                                           ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))),
                                                                                                                                                  ![__s] = Tail(@)]
                                                                                                   /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                   /\ UNCHANGED __max_loss
                                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                   /\ __max_loss > 0
                                                                                                   /\ __max_loss' = __max_loss - 1
                                                                                                   /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                   /\ UNCHANGED __max_out_of_order
                                                                                                \/ /\ IF __s = __h
                                                                                                         THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))))]
                                                                                                         ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))),
                                                                                                                                                ![__s] = Tail(@)]
                                                                                                   /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                    ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                              Assert(__b, 
                                                                                                     "Failure of assertion at line 122, column 7 of macro called at line 519, column 21.")
                                                                                         /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                         /\ UNCHANGED << __max_loss, 
                                                                                                         __max_out_of_order >>
                                                                            /\ UNCHANGED << BEPsn, 
                                                                                            BBuffer, 
                                                                                            nak_sent, 
                                                                                            recirc_pkts >>
                                                                       ELSE /\ IF (pkt).psn > BEPsn[__Node(self)]
                                                                                  THEN /\ IF ~nak_sent[__Node(self)]
                                                                                             THEN /\ LET next == __next_hop[__Node(self), (MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))).dst] IN
                                                                                                       /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))
                                                                                                             THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                                       Assert(__b, 
                                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 529, column 23.")
                                                                                                                  /\ LET __s == __Node(self) IN
                                                                                                                       LET __h == __next_hop[__s, ((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)])))).dst] IN
                                                                                                                         \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                            /\ __max_out_of_order > 0
                                                                                                                            /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                                            /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                                                 IF __s = __h
                                                                                                                                    THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))))]
                                                                                                                                    ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))),
                                                                                                                                                                           ![__s] = Tail(@)]
                                                                                                                            /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                                            /\ UNCHANGED __max_loss
                                                                                                                         \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                            /\ __max_loss > 0
                                                                                                                            /\ __max_loss' = __max_loss - 1
                                                                                                                            /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                                            /\ UNCHANGED __max_out_of_order
                                                                                                                         \/ /\ IF __s = __h
                                                                                                                                  THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))))]
                                                                                                                                  ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((NAK), (__Node(self)), ((pkt).src), (BEPsn[__Node(self)]))))),
                                                                                                                                                                         ![__s] = Tail(@)]
                                                                                                                            /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                                             ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                                       Assert(__b, 
                                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 532, column 23.")
                                                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                                                  __max_out_of_order >>
                                                                                                       /\ nak_sent' = [nak_sent EXCEPT ![__Node(self)] = TRUE]
                                                                                             ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 538, column 19.")
                                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                                  __max_out_of_order, 
                                                                                                                  nak_sent >>
                                                                                       /\ UNCHANGED << BEPsn, 
                                                                                                       BBuffer, 
                                                                                                       recirc_pkts >>
                                                                                  ELSE /\ nak_sent' = [nak_sent EXCEPT ![__Node(self)] = FALSE]
                                                                                       /\ IF (pkt).psn >= BPsnStart[__Node(self)] + AGG_NUM
                                                                                             THEN /\ TRUE
                                                                                                  /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 545, column 19.")
                                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                                  __max_out_of_order, 
                                                                                                                  BEPsn, 
                                                                                                                  BBuffer, 
                                                                                                                  recirc_pkts >>
                                                                                             ELSE /\ BBuffer' = [BBuffer EXCEPT ![__Node(self)][idx] = (pkt).value]
                                                                                                  /\ BEPsn' = [BEPsn EXCEPT ![__Node(self)] = BEPsn[__Node(self)] + 1]
                                                                                                  /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropMulticast: empty buffer") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 550, column 19.")
                                                                                                  /\ LET __b == Assert(((ChildrenOf(__Node(self))) \subseteq __links[__Node(self)]), "DropMulticast: invalid destinations") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 550, column 19.")
                                                                                                  /\ LET __b == Assert((__Node(self) \notin (ChildrenOf(__Node(self)))), "DropMulticast: self in destinations") IN
                                                                                                       Assert(__b, 
                                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 550, column 19.")
                                                                                                  /\ LET __pkts == [__dst \in (ChildrenOf(__Node(self))) |-> "dst" :> __dst @@ (MakePktWithValue(RES, __Node(self), __Node(self), (pkt).psn, (pkt).value))] IN
                                                                                                       LET __possible_losses_1 == __AllPossibleLoss((ChildrenOf(__Node(self)))) IN
                                                                                                         LET __possible_losses_2 ==                     {__l \in __possible_losses_1 :
                                                                                                                                    \A __d \in __l : __IsUnreliableLink(__Node(self), __d)} IN
                                                                                                           \E __loss \in __possible_losses_2:
                                                                                                             LET __unlost_dsts == (ChildrenOf(__Node(self))) \ __loss IN
                                                                                                               LET __possible_ooo_1 == __AllPossibleOutOfOrder(__unlost_dsts) IN
                                                                                                                 LET __possible_ooo_2 ==                  {__o \in __possible_ooo_1 :
                                                                                                                                         \A __d \in DOMAIN __o : __o[__d] > 0 => __IsUnreliableLink(__Node(self), __d)} IN
                                                                                                                   \E __ooo \in __possible_ooo_2:
                                                                                                                     /\ __net_buf' =              [__n \in NODE_SET |->
                                                                                                                                       CASE __n \in __unlost_dsts -> __InsertAtEnd(__net_buf[__n], __ooo[__n], __pkts[__n])
                                                                                                                                       [] __n = __Node(self) -> Tail(__net_buf[__n])
                                                                                                                                       [] OTHER -> __net_buf[__n]
                                                                                                                                     ]
                                                                                                                     /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                                                     /\ __max_out_of_order' = __max_out_of_order - __PosCount(__ooo)
                                                                                                  /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = Append(recirc_pkts[__Node(self)], (MakePkt(ACK, __Node(self), (pkt).src, BEPsn'[__Node(self)])))]
                                                            /\ UNCHANGED << BPsnStart, 
                                                                            REPsn >>
                                                       ELSE /\ IF pkt.type = ACK
                                                                  THEN /\ LET idx == Idx((pkt).psn) IN
                                                                            LET ch == (pkt).src IN
                                                                              /\ LET __b == Assert((ch \in ChildrenOf(__Node(self))), "SwitchAck: switch receives ACK from non-child.") IN
                                                                                   Assert(__b, 
                                                                                          "Failure of assertion at line 122, column 7 of macro called at line 561, column 15.")
                                                                              /\ LET __b == Assert((BEPsn[__Node(self)] >= (pkt).psn), "SwitchAck: switch receives ACK for unreceived result.") IN
                                                                                   Assert(__b, 
                                                                                          "Failure of assertion at line 122, column 7 of macro called at line 562, column 15.")
                                                                              /\ IF (pkt).psn > REPsn[__Node(self)][ch]
                                                                                    THEN /\ LET new_start == MinEPsn(__Node(self), ch :> (pkt).psn @@ REPsn[__Node(self)]) IN
                                                                                              LET range == BPsnStart[__Node(self)] .. (new_start - 1) IN
                                                                                                /\ REPsn' = [REPsn EXCEPT ![__Node(self)][ch] = (pkt).psn]
                                                                                                /\ IF new_start > BPsnStart[__Node(self)]
                                                                                                      THEN /\ BBuffer' = [BBuffer EXCEPT ![__Node(self)] = [i \in PsnSetToIdxSet(range) |-> 0] @@ BBuffer[__Node(self)]]
                                                                                                           /\ BPsnStart' = [BPsnStart EXCEPT ![__Node(self)] = new_start]
                                                                                                      ELSE /\ TRUE
                                                                                                           /\ UNCHANGED << BBuffer, 
                                                                                                                           BPsnStart >>
                                                                                    ELSE /\ TRUE
                                                                                         /\ UNCHANGED << BBuffer, 
                                                                                                         BPsnStart, 
                                                                                                         REPsn >>
                                                                              /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                   Assert(__b, 
                                                                                          "Failure of assertion at line 122, column 7 of macro called at line 575, column 15.")
                                                                              /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ UNCHANGED recirc_pkts
                                                                  ELSE /\ IF pkt.type = NAK
                                                                             THEN /\ LET idx == Idx((pkt).psn) IN
                                                                                       LET ch == (pkt).src IN
                                                                                         LET max_psn == BEPsn[__Node(self)] - 1 IN
                                                                                           LET range_seq == RangeToSeq(REPsn[__Node(self)][ch], max_psn) IN
                                                                                             LET pkts == [i \in 1 .. Len(range_seq) |-> MakePktWithValue(RES, __Node(self), ch, range_seq[i], BBuffer[__Node(self)][Idx(range_seq[i])])] IN
                                                                                               /\ LET __b == Assert((ch \in ChildrenOf(__Node(self))), "SwitchNak: switch receives NAK from non-child.") IN
                                                                                                    Assert(__b, 
                                                                                                           "Failure of assertion at line 122, column 7 of macro called at line 586, column 15.")
                                                                                               /\ LET __b == Assert(((pkt).psn = REPsn[__Node(self)][ch]), "SwitchNak: we currently do not consider out-of-order networks.") IN
                                                                                                    Assert(__b, 
                                                                                                           "Failure of assertion at line 122, column 7 of macro called at line 587, column 15.")
                                                                                               /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = recirc_pkts[__Node(self)] \o (pkts)]
                                                                                               /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                    Assert(__b, 
                                                                                                           "Failure of assertion at line 122, column 7 of macro called at line 589, column 15.")
                                                                                               /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                             ELSE /\ LET __b == Assert(FALSE, "SwitchRecv: unexpected packet type") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 593, column 13.")
                                                                                  /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 122, column 7 of macro called at line 594, column 13.")
                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                  /\ UNCHANGED recirc_pkts
                                                                       /\ UNCHANGED << BBuffer, 
                                                                                       BPsnStart, 
                                                                                       REPsn >>
                                                            /\ UNCHANGED << __max_loss, 
                                                                            __max_out_of_order, 
                                                                            BEPsn, 
                                                                            nak_sent >>
                                            /\ pc' = [pc EXCEPT ![self] = "L_SwitchRecv"]
                                 /\ UNCHANGED __active_threads
                            ELSE /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                 /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order, BEPsn, 
                                                 BBuffer, BPsnStart, REPsn, 
                                                 nak_sent, recirc_pkts >>
                      /\ UNCHANGED << __max_duplication, res, requests, base, 
                                      res_epsn >>

SwitchRecv(self) == L_SwitchRecv(self)

L_SwitchRecirc(self) == /\ pc[self] = "L_SwitchRecirc"
                        /\ IF ~SwitchCanTerminate(__Node(self))
                              THEN /\ IF __active_threads[__Node(self)] <= 0
                                         THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                              /\ UNCHANGED << __net_buf, 
                                                              __max_loss, 
                                                              __max_out_of_order, 
                                                              __max_duplication, 
                                                              recirc_pkts >>
                                         ELSE /\ LET next == __next_hop[__Node(self), (Head(recirc_pkts[__Node(self)])).dst] IN
                                                   /\ recirc_pkts[__Node(self)] # <<>>
                                                   /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (Head(recirc_pkts[__Node(self)])))
                                                         THEN /\ LET has_dup == /\ "id" \in DOMAIN ((Head(recirc_pkts[__Node(self)])))
                                                                                /\ \E __n \in NODE_SET : \E __i \in DOMAIN __net_buf[__n] :
                                                                                  "id" \in DOMAIN __net_buf[__n][__i] /\ __net_buf[__n][__i].id = ((Head(recirc_pkts[__Node(self)]))).id IN
                                                                   IF ~has_dup
                                                                      THEN /\ LET __h == __next_hop[__Node(self), ((Head(recirc_pkts[__Node(self)]))).dst] IN
                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                   /\ __max_out_of_order > 0
                                                                                   /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                   /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                        __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                   /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                   /\ UNCHANGED __max_loss
                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                   /\ __max_loss > 0
                                                                                   /\ __max_loss' = __max_loss - 1
                                                                                   /\ UNCHANGED <<__net_buf, __max_out_of_order>>
                                                                                \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                   /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                           /\ UNCHANGED __max_duplication
                                                                      ELSE /\ \/ /\ __max_duplication > 0
                                                                                 /\ __max_duplication' = __max_duplication - 1
                                                                                 /\ LET __h == __next_hop[__Node(self), ((Head(recirc_pkts[__Node(self)]))).dst] IN
                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                         /\ __max_out_of_order > 0
                                                                                         /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                         /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                              __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                         /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                         /\ UNCHANGED __max_loss
                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                         /\ __max_loss > 0
                                                                                         /\ __max_loss' = __max_loss - 1
                                                                                         /\ UNCHANGED <<__net_buf, __max_out_of_order>>
                                                                                      \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                         /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                              \/ /\ TRUE
                                                                                 /\ UNCHANGED <<__net_buf, __max_loss, __max_out_of_order, __max_duplication>>
                                                         ELSE /\ TRUE
                                                              /\ UNCHANGED << __net_buf, 
                                                                              __max_loss, 
                                                                              __max_out_of_order, 
                                                                              __max_duplication >>
                                                   /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = Tail(recirc_pkts[__Node(self)])]
                                              /\ pc' = [pc EXCEPT ![self] = "L_SwitchRecirc"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                   /\ UNCHANGED << __net_buf, __max_loss, 
                                                   __max_out_of_order, 
                                                   __max_duplication, 
                                                   recirc_pkts >>
                        /\ UNCHANGED << res, requests, base, __active_threads, 
                                        BEPsn, BBuffer, BPsnStart, REPsn, 
                                        nak_sent, res_epsn >>

SwitchRecirc(self) == L_SwitchRecirc(self)

L_SwitchRetxRes(self) == /\ pc[self] = "L_SwitchRetxRes"
                         /\ IF ~SwitchCanTerminate(__Node(self))
                               THEN /\ IF __active_threads[__Node(self)] <= 0
                                          THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                               /\ UNCHANGED recirc_pkts
                                          ELSE /\ LET max_psn == BEPsn[__Node(self)] - 1 IN
                                                    LET active_children == {ch \in ChildrenOf(__Node(self)) : ~__NodeTerminated(ch)} IN
                                                      LET range_seq == [ch \in active_children |-> RangeToSeq(REPsn[__Node(self)][ch], max_psn)] IN
                                                        LET pkts == MergeFuncOfSeq([ch \in active_children |-> [i \in 1 .. Len(range_seq[ch]) |-> MakePktWithValue(RES, __Node(self), ch, range_seq[ch][i], BBuffer[__Node(self)][Idx(range_seq[ch][i])])]]) IN
                                                          /\ recirc_pkts[__Node(self)] = <<>>
                                                          /\ StrictRetxCondition(0)
                                                          /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = recirc_pkts[__Node(self)] \o (pkts)]
                                               /\ pc' = [pc EXCEPT ![self] = "L_SwitchRetxRes"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ UNCHANGED recirc_pkts
                         /\ UNCHANGED << __net_buf, __max_loss, 
                                         __max_out_of_order, __max_duplication, 
                                         res, requests, base, __active_threads, 
                                         BEPsn, BBuffer, BPsnStart, REPsn, 
                                         nak_sent, res_epsn >>

SwitchRetxRes(self) == L_SwitchRetxRes(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in (CLIENT_SET \X {"ClientInitSend"}): ClientInitSend(self))
           \/ (\E self \in (CLIENT_SET \X {"ClientRecv"}): ClientRecv(self))
           \/ (\E self \in (CLIENT_SET \X {"ClientRetx"}): ClientRetx(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRecv"}): SwitchRecv(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRecirc"}): SwitchRecirc(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRetxRes"}): SwitchRetxRes(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Next)
        /\ \A self \in (CLIENT_SET \X {"ClientInitSend"}) : SF_vars(ClientInitSend(self))
        /\ \A self \in (CLIENT_SET \X {"ClientRecv"}) : SF_vars(ClientRecv(self))
        /\ \A self \in (CLIENT_SET \X {"ClientRetx"}) : SF_vars(ClientRetx(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRecv"}) : SF_vars(SwitchRecv(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRecirc"}) : SF_vars(SwitchRecirc(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRetxRes"}) : SF_vars(SwitchRetxRes(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

BEPsnInRange == \A nid \in SWITCH_SET : BEPsn[nid] >= BPsnStart[nid] /\ BEPsn[nid] <= BPsnStart[nid] + AGG_NUM
REPsnInRange == \A nid \in SWITCH_SET : \A ch \in ChildrenOf(nid) : REPsn[nid][ch] >= BPsnStart[nid] /\ REPsn[nid][ch] <= BPsnStart[nid] + AGG_NUM

====
