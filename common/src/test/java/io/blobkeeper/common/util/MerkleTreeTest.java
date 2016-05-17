package io.blobkeeper.common.util;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import io.blobkeeper.common.service.IdGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static java.util.Collections.max;
import static java.util.Collections.min;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MerkleTreeTest {
    private static final Logger log = LoggerFactory.getLogger(MerkleTreeTest.class);

    private Random random = new Random();

    @Test
    public void buildTreeEmpty() {
        MerkleTree tree = Utils.createEmptyTree(Range.openClosed(1L, 100L), 3);

        LeafNode node1 = new LeafNode(Range.openClosed(1L, 50L));
        node1.addHash(HashableNode.EMPTY_HASH, 0);
        LeafNode node2 = new LeafNode(Range.openClosed(50L, 100L));
        node2.addHash(HashableNode.EMPTY_HASH, 0);

        assertEquals(tree.getLeafNodes(), ImmutableList.of(node1, node2));
        assertNotNull(tree.getRoot().getHash());
    }

    @Test
    public void buildTree() {
        Block block = new Block(1L, Arrays.asList(new BlockElt(1, 0, 2, 3, 4)));
        SortedMap<Long, Block> blocks = ImmutableSortedMap.of(42L, block);

        MerkleTree tree = Utils.createTree(Range.openClosed(1L, 100L), 3, blocks);
        tree.getLeafNodes();

        LeafNode node1 = new LeafNode(Range.openClosed(1L, 50L));
        node1.addHash(new byte[]{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}, 1);

        LeafNode node2 = new LeafNode(Range.openClosed(50L, 100L));
        node2.addHash(HashableNode.EMPTY_HASH, 0);

        assertEquals(tree.getLeafNodes(), ImmutableList.of(node1, node2));
        assertNotNull(tree.getRoot().getHash());
    }

    @Test
    public void removeElementAtTheEnd() {
        Long[] idsArray = new Long[]{303277865741324292L, 303277865741324291L, 303277865741324289L, 303277863761612800L,
                303277863707086848L, 303277863488983040L, 303277863350571008L, 303277861882564608L, 303277861677043712L,
                303277794798866449L, 303277794798866447L, 303277794798866446L, 303277794798866444L, 303277794798866442L,
                303277794798866439L, 303277794798866438L, 303277794798866435L, 303277794798866432L, 303277792722685954L,
                303277792722685953L, 303277792567496704L, 303277792559108097L, 303277792559108096L, 303277790935912454L,
                303277790935912453L, 303277790935912451L, 303277790935912448L, 303277790931718146L, 303277790931718144L,
                303277790919135238L, 303277790919135237L, 303277790919135236L, 303277790919135234L, 303277790914940945L,
                303277790914940943L, 303277790914940942L, 303277790914940940L, 303277790914940938L, 303277790914940936L,
                303277790914940934L, 303277790914940931L, 303277790914940929L, 303277790302572557L, 303277790302572556L,
                303277790302572554L, 303277790302572550L, 303277790302572549L, 303277790302572548L, 303277790302572544L,
                303277789690204160L, 303277789396602884L, 303277789396602881L, 303277789249802243L, 303277789249802242L,
                303277789249802241L, 303277789115584517L, 303277789115584516L, 303277788947812352L, 303277788939423744L,
                303277788780040192L, 303277707867721735L, 303277707867721732L, 303277707867721731L, 303277707867721729L,
                303277706680733696L, 303277706290663425L, 303277706231943170L, 303277706231943168L, 303277706206777344L,
                303277706143862784L, 303277705997062145L, 303277705766375424L, 303277705665712131L, 303277705665712128L,
                303277704453558301L, 303277704453558300L, 303277704453558297L, 303277704453558296L, 303277704453558295L,
                303277704453558292L, 303277704453558290L, 303277704453558286L, 303277704453558284L, 303277704453558281L,
                303277704453558280L, 303277704453558279L, 303277704453558277L, 303277704453558275L, 303277704306757660L,
                303277704306757658L, 303277704306757656L, 303277704306757654L, 303277704306757650L, 303277704306757648L,
                303277704306757647L, 303277704306757645L, 303277704306757644L, 303277704306757643L, 303277704306757641L,
                303277704306757638L, 303277704306757637L, 303277704306757634L, 303277703153324042L, 303277703153324040L,
                303277703153324038L, 303277703153324037L, 303277703153324034L, 303277703153324032L, 303277702675173376L,
                303277665366839297L, 303277665358450711L, 303277665358450709L, 303277665358450707L, 303277665358450705L,
                303277665358450703L, 303277665358450699L, 303277665358450698L, 303277665358450696L, 303277665358450693L,
                303277665358450692L, 303277665358450690L, 303277665358450688L, 303277662225305605L, 303277662225305603L,
                303277662225305602L, 303277662141419522L, 303277662141419521L, 303277661961064448L, 303277660581138438L,
                303277660581138435L, 303277660581138434L, 303277660581138432L, 303277660576944138L, 303277660576944134L,
                303277660576944133L, 303277660576944132L, 303277660576944131L, 303277660576944129L, 303277660493058060L,
                303277660493058058L, 303277660493058056L, 303277660493058055L, 303277660493058053L, 303277660493058051L,
                303277660488863752L, 303277660488863750L, 303277660488863749L, 303277660488863748L, 303277660488863746L,
                303277659775832064L, 303277659285098497L, 303277659150880770L, 303277659150880769L, 303277659058606080L,
                303277658936971265L, 303277658924388352L, 303277658475597824L, 303277658173607936L, 303277657968087040L,
                303277627441942543L, 303277627441942542L, 303277627441942540L, 303277627441942538L, 303277627441942537L,
                303277627441942535L, 303277627441942532L, 303277627441942531L, 303277627441942529L, 303277627353862147L,
                303277627353862146L, 303277627353862144L, 303277626766659586L, 303277626766659585L, 303277626523389952L,
                303277625734860827L, 303277625734860824L, 303277625734860822L, 303277625734860821L, 303277625734860820L,
                303277625734860819L, 303277625734860815L, 303277625734860814L, 303277625734860813L, 303277625734860810L,
                303277625734860809L, 303277625734860807L, 303277625734860805L, 303277625734860802L, 303277625734860801L,
                303277625730666507L, 303277625730666505L, 303277625730666504L, 303277625730666503L, 303277625730666501L,
                303277625730666498L, 303277625730666497L, 303277625726472224L, 303277625726472222L, 303277625726472221L,
                303277625726472213L, 303277625726472212L, 303277625726472211L, 303277625726472210L, 303277625726472208L,
                303277625726472206L, 303277625726472205L, 303277625726472204L, 303277625726472203L, 303277625726472202L,
                303277625726472201L, 303277625726472200L, 303277625726472196L, 303277608265584640L, 303277608261390337L,
                303277608261390336L, 303277608248807431L, 303277608248807430L, 303277608248807425L, 303277608248807424L,
                303277607883902976L, 303277607472861190L, 303277607472861188L, 303277607472861186L, 303277607472861185L,
                303277607451889664L, 303277605824499749L, 303277605824499747L, 303277605824499745L, 303277605824499744L,
                303277605824499741L, 303277605824499739L, 303277605824499738L, 303277605824499737L, 303277605824499735L,
                303277605824499733L, 303277605824499729L, 303277605824499728L, 303277605824499727L, 303277605824499723L,
                303277605824499720L, 303277605824499719L, 303277605824499716L, 303277605824499715L, 303277605824499712L,
                303277605820305409L, 303277605493149707L, 303277605493149706L, 303277605493149704L, 303277605493149701L,
                303277605493149700L, 303277605493149698L, 303277604805283843L, 303277604532654080L, 303277604234858496L,
                303277604146778114L, 303277603869954048L, 303277603861565444L, 303277603861565443L, 303277603861565440L,
                303277603790262272L, 303277554150674437L, 303277554150674436L, 303277554150674434L, 303277554146480129L,
                303277554133897227L, 303277554133897224L, 303277554133897221L, 303277554133897217L, 303277554133897216L,
                303277553383116800L, 303277553274064896L, 303277552439398410L, 303277552439398408L, 303277552439398407L,
                303277552439398405L, 303277552439398404L, 303277552439398403L, 303277552439398401L, 303277550350635023L,
                303277550350635020L, 303277550350635018L, 303277550350635017L, 303277550350635014L, 303277550350635013L,
                303277550350635010L, 303277550346440706L, 303277550346440705L, 303277550346440704L, 303277550241583109L,
                303277550241583108L, 303277550241583107L, 303277550241583106L, 303277550241583105L, 303277550237388807L,
                303277550237388804L, 303277550237388803L, 303277550237388800L, 303277549406916612L, 303277549406916611L,
                303277549406916609L, 303277549402722304L, 303277549071372289L, 303277548786159617L, 303277548786159616L,
                303277548635164678L, 303277548635164677L, 303277548635164675L, 303277548635164674L, 303277548635164673L,
                303277548479975424L, 303277518004162578L, 303277518004162577L, 303277518004162575L, 303277518004162573L,
                303277518004162572L, 303277518004162571L, 303277518004162568L, 303277518004162567L, 303277518004162564L,
                303277518004162562L, 303277518004162561L, 303277517999968291L, 303277517999968288L, 303277517999968287L,
                303277517999968285L, 303277517999968284L, 303277517999968281L, 303277517999968278L, 303277517999968277L,
                303277517999968274L, 303277517999968273L, 303277517999968271L, 303277517999968269L, 303277517999968267L,
                303277517999968263L, 303277517999968262L, 303277517999968261L, 303277517999968259L, 303277517391794176L,
                303277516368384000L, 303277516359995398L, 303277516359995397L, 303277516359995396L, 303277516359995394L,
                303277515672129537L, 303277515672129536L, 303277515621797891L, 303277515621797890L, 303277515164618753L,
                303277515164618752L, 303277515017818112L, 303277512048250881L, 303277512035667973L, 303277512035667971L,
                303277512035667969L, 303277511804981248L, 303277511582683136L, 303277511570100225L, 303277511343607808L,
                303277511230361600L, 303277511159058433L, 303275919756234760L, 303275919756234756L, 303275919756234754L,
                303275919756234753L, 303275919752040476L, 303275919752040475L, 303275919752040473L, 303275919752040469L,
                303275919752040468L, 303275919752040465L, 303275919752040463L, 303275919752040461L, 303275919752040459L,
                303275919752040457L, 303275919752040454L, 303275919752040452L, 303275919752040450L, 303275919752040448L,
                303275919747846146L, 303275919747846145L, 303275919353581568L, 303275919349387282L, 303275919349387280L,
                303275919349387277L, 303275919349387276L, 303275919349387275L, 303275919349387274L, 303275919349387272L,
                303275919349387270L, 303275919349387269L, 303275919349387266L, 303275918137233411L, 303275918137233409L,
                303275917659082752L, 303275917654888450L, 303275917524865028L, 303275917524865026L, 303275917524865024L,
                303275917306761217L, 303275917138989056L, 303275916962828289L, 303275916950245378L, 303275916950245377L,
                303275916593729537L, 303275916589535232L, 303275916199464960L, 303275911329878018L, 303275911321489408L,
                303275832112058387L, 303275832112058385L, 303275832112058384L, 303275832112058383L, 303275832112058380L,
                303275832112058379L, 303275832112058377L, 303275832112058374L, 303275832112058373L, 303275832112058368L,
                303275831713599493L, 303275831713599489L, 303275831713599488L, 303275831365472256L, 303275830669217792L,
                303275830266564608L, 303275829066993664L, 303275828987301921L, 303275828987301919L, 303275828987301916L,
                303275828987301914L, 303275828987301912L, 303275828987301910L, 303275828987301908L, 303275828987301907L,
                303275828987301905L, 303275828987301904L, 303275828987301901L, 303275828987301898L, 303275828987301896L,
                303275828987301894L, 303275828987301893L, 303275828987301890L, 303275828987301889L, 303275828156829704L,
                303275828156829703L, 303275828156829700L, 303275828156829699L, 303275828156829698L, 303275827804508162L,
                303275827804508160L, 303275827800313860L, 303275827800313859L, 303275827800313857L, 303275827494129664L,
                303275827196334085L, 303275827196334082L, 303275827196334081L, 303275827095670784L, 303275559247417365L,
                303275559247417362L, 303275559247417361L, 303275559247417360L, 303275559247417359L, 303275559247417355L,
                303275559247417354L, 303275559247417350L, 303275559247417347L, 303275559247417345L, 303275559243223057L,
                303275559243223056L, 303275559243223053L, 303275559243223049L, 303275559243223048L, 303275559243223046L,
                303275559243223045L, 303275559243223044L, 303275559243223043L, 303275558844764163L, 303275558844764162L,
                303275558844764161L, 303275558844764160L, 303275558840569864L, 303275558840569862L, 303275558840569861L,
                303275558840569860L, 303275558840569859L, 303275558840569857L, 303275557917822986L, 303275557917822985L,
                303275557917822982L, 303275557917822981L, 303275557917822978L, 303275557917822977L, 303275557183819782L,
                303275557183819781L, 303275557183819780L, 303275557183819779L, 303275557183819777L, 303275556353347584L,
                303275556261072896L, 303275556047163394L, 303275556047163392L, 303275555950694400L, 303275555740979200L,
                303275554423967745L, 303275554423967744L, 303275550380658694L, 303275550380658691L, 303275550380658688L,
                303275550321938432L, 303274580351389724L, 303274580351389722L, 303274580351389719L, 303274580351389718L,
                303274580351389717L, 303274580351389714L, 303274580351389712L, 303274580351389710L, 303274580351389709L,
                303274580351389707L, 303274580351389706L, 303274580351389701L, 303274580351389698L, 303274580351389697L,
                303274580351389696L, 303274580099731462L, 303274580099731461L, 303274580099731458L, 303274580099731456L,
                303274579462197250L, 303274579462197249L, 303274579239899136L, 303274579105681408L, 303274578963075072L,
                303274578732388352L, 303274578099048455L, 303274578099048453L, 303274578099048452L, 303274578099048449L,
                303274575536328712L, 303274575536328711L, 303274575536328708L, 303274575536328707L, 303274575536328705L,
                303274575536328704L, 303274575532134404L, 303274575532134403L, 303274575532134400L, 303274575368556557L,
                303274575368556554L, 303274575368556552L, 303274575368556551L, 303274575368556550L, 303274575368556548L,
                303274575368556545L, 303274574844268553L, 303274574844268552L, 303274574844268550L, 303274574844268549L,
                303274574844268546L, 303274574844268544L, 303274574504529920L, 303274501129375748L, 303274501129375746L,
                303274501079044097L, 303274501079044096L, 303274498726039552L, 303274498721845251L, 303274498721845250L,
                303274498721845249L, 303274497153175552L, 303274496993792000L, 303274448776073216L, 303274448763490304L,
                303274445403852807L, 303274445403852805L, 303274445403852804L, 303274445403852803L, 303274445403852802L,
                303274445252857857L, 303274445252857856L, 303274444745347072L, 303274294014644225L};

        List<Long> ids = new ArrayList<>(Arrays.asList(idsArray));
        long min = min(ids);
        long max = max(ids);
        Collections.sort(ids);

        Range<Long> ranges = Range.openClosed(min - 1, max);

        SortedMap<Long, Block> blocks = ids.stream()
                .map(id -> new Block(id, Arrays.asList(new BlockElt(id, 1, 0, 0, 0))))
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        MerkleTree tree = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree, blocks);
        tree.calculate();

        // expected diff is last 8 elements from the end in the sorted array
        Set<Long> expectedDiff = ids.stream().skip(ids.size() - 8)
                .collect(GuavaCollectors.toImmutableSet());

        // remove elt at position 581
        ids.remove(581);

        SortedMap<Long, Block> blocks2 = ids.stream()
                .map(id -> new Block(id, Arrays.asList(new BlockElt(id, 1, 0, 0, 0))))
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        MerkleTree tree2 = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree2, blocks2);
        tree2.calculate();

        List<LeafNode> diff = MerkleTree.difference(tree, tree2).stream()
                .collect(toImmutableList());

        assertEquals(diff.size(), 1);

        assertEquals(expectedDiff.stream()
                        .filter(elt -> diff.get(0).getRange().contains(elt))
                        .count(),
                expectedDiff.size()
        );
    }

    @Test
    public void differenceInTypes() throws InterruptedException {
        IdGeneratorService service = new IdGeneratorService();

        List<Block> originalBlocks = new ArrayList<>();

        long min = 0, max = 0;

        int offset = 0;
        for (int i = 0; i < 100; i++) {
            long id = service.generate(1);
            int length = random.nextInt(65536);
            originalBlocks.add(
                    new Block(
                            id,
                            Arrays.asList(
                                    new BlockElt(id, 0, offset, length, 42),
                                    new BlockElt(id, 1, offset, length, 42)
                            )
                    )
            );
            offset += length;

            if (min == 0) {
                min = id;
            }
            max = id;

            Thread.sleep(1);
        }

        SortedMap<Long, Block> blocks = originalBlocks.stream()
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        SortedMap<Long, Block> blocks2 = originalBlocks.stream()
                // remove block elts
                .map(block -> {
                    if (block.getId() % 99 == 0) {
                        return new Block(block.getId(), block.getBlockElts().stream().limit(1).collect(toImmutableList()));
                    }
                    return block;
                })
                .collect(toMap(Block::getId, Function.identity(), Utils.throwingMerger(), TreeMap::new));

        Range<Long> ranges = Range.openClosed(min - 1, max);

        MerkleTree tree = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree, blocks);

        MerkleTree tree2 = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree2, blocks2);

        List<LeafNode> diff = MerkleTree.difference(tree, tree2).stream()
                .collect(toImmutableList());

        assertEquals(diff.size(), 1);
    }

    @Test
    public void difference() throws InterruptedException {
        IdGeneratorService service = new IdGeneratorService();

        List<Block> originalBlocks = new ArrayList<>();

        long min = 0, max = 0;

        int offset = 0;
        for (int i = 0; i < 10000; i++) {
            long id = service.generate(1);
            int length = random.nextInt(65536);
            originalBlocks.add(new Block(id, Arrays.asList(new BlockElt(id, 0, offset, length, 42))));
            offset += length;

            if (min == 0) {
                min = id;
            }
            max = id;

            Thread.sleep(1);
        }

        SortedMap<Long, Block> blocks = originalBlocks.stream()
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        SortedMap<Long, Block> blocks2 = originalBlocks.stream()
                .limit(9500)
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        Range<Long> ranges = Range.openClosed(min - 1, max);

        MerkleTree tree = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree, blocks);

        MerkleTree tree2 = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree2, blocks2);

        assertEquals(
                MerkleTree.difference(tree, tree2)
                        .stream()
                        .count(),
                4);
    }
}
