package com.instaclustr.esop.impl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SSTableUtilsTest {
    @Test
    void verifySSTableRegex() {
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("instaclustr-recovery_codes-jb-1-Data.db").matches());
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("lb-1-big-Data.db").matches());
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("mc-1-big-Data.db").matches());
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("da-3gy0_111j_2u8ka1z4k2vcw67ubc-bti-Data.db").matches());
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("da-1-bti-Data.db").matches());
        assertTrue(SSTableUtils.SSTABLE_RE.matcher("instaclustr-recovery_codes-jb-1-Data.db").matches());
    }
}