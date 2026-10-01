package com.trainstar.synchro

import java.math.BigDecimal
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ModelsTests {
    private val twoPow53 = 9_007_199_254_740_992L
    // Doubles in [2^62, 2^63) are 1024 apart, so this is the largest Double below 2^63.
    private val largestDoubleBelowTwoPow63 = 9_223_372_036_854_774_784L
    private val twoPow63 = 9.223372036854775808E18

    @Test
    fun equalNumberFormsShareEqualityHashAndLookup() {
        val groups = listOf(
            listOf<Any>(5, 5L, 5.0, 5.0f),
            listOf<Any>(-1, -1L, -1.0, -1.0f),
            listOf<Any>(0, 0L, 0.0, -0.0, 0.0f, -0.0f),
            listOf<Any>(0.5, 0.5f),
            listOf<Any>(twoPow53, 9.007199254740992E15),
            listOf<Any>(largestDoubleBelowTwoPow63, 9.223372036854774784E18),
            listOf<Any>(Long.MIN_VALUE, -9.223372036854775808E18),
            listOf<Any>(twoPow63, twoPow63.toFloat()),
            // The serializer writes other Number types as text, so they keep their own equality.
            listOf<Any>(BigDecimal("1.5"), BigDecimal("1.5")),
        )
        for (group in groups) {
            val forms = group.map(::AnyCodable)
            val first = forms.first()
            val set = hashSetOf(first)
            val map = hashMapOf(first to "found")
            for (form in forms) {
                assertEquals(first, form)
                assertEquals(form, first)
                assertEquals("hash of $form and $first", first.hashCode(), form.hashCode())
                assertTrue("set lookup of $form", form in set)
                assertEquals("map lookup of $form", "found", map[form])
            }
            assertEquals("distinct values in $forms", 1, forms.toHashSet().size)
        }
    }

    @Test
    fun distinctValuesStayDistinctAcrossForms() {
        // Each Long rounds to its paired Double, so a Double comparison would call them equal.
        assertEquals(9.007199254740992E15, (twoPow53 + 1).toDouble(), 0.0)
        assertEquals(9.223372036854774784E18, (largestDoubleBelowTwoPow63 + 1).toDouble(), 0.0)
        assertEquals(twoPow63, Long.MAX_VALUE.toDouble(), 0.0)
        assertEquals(-9.223372036854775808E18, (Long.MIN_VALUE + 1).toDouble(), 0.0)
        // A Long conversion truncates this value to its paired Long.
        assertEquals(1L, BigDecimal("1.5").toLong())

        val pairs = listOf<Pair<Any?, Any?>>(
            twoPow53 + 1 to 9.007199254740992E15,
            twoPow53 + 1 to twoPow53,
            largestDoubleBelowTwoPow63 + 1 to 9.223372036854774784E18,
            Long.MAX_VALUE to twoPow63,
            Long.MAX_VALUE to twoPow63.toFloat(),
            Long.MIN_VALUE + 1 to -9.223372036854775808E18,
            1L to 1.5,
            2L to 1.5,
            0 to 0.5f,
            0.1f to 0.1,
            1L to true,
            0L to false,
            1L to "1",
            -0.0 to "0",
            0L to null,
            BigDecimal("1.5") to 1L,
            5.toShort() to 5L,
            5.toShort() to 5.0,
        )
        for ((left, right) in pairs) {
            assertNotEquals(AnyCodable(left), AnyCodable(right))
            assertNotEquals(AnyCodable(right), AnyCodable(left))
            assertEquals("distinct values $left and $right", 2, hashSetOf(AnyCodable(left), AnyCodable(right)).size)
        }
    }

    @Test
    fun wrappedChildrenFollowTheSameRule() {
        val list = AnyCodable(listOf(AnyCodable(5L), AnyCodable(-0.0), AnyCodable("x")))
        val sameList = AnyCodable(listOf(AnyCodable(5.0), AnyCodable(0), AnyCodable("x")))
        val map = AnyCodable(mapOf("n" to AnyCodable(-1), "items" to list))
        val sameMap = AnyCodable(mapOf("n" to AnyCodable(-1.0), "items" to sameList))
        // Record data maps hold wrapped values directly.
        val data = mapOf("score" to AnyCodable(2L), "delta" to AnyCodable(0.0))
        val sameData = mapOf("score" to AnyCodable(2.0f), "delta" to AnyCodable(-0.0))
        for ((left, right) in listOf<Pair<Any, Any>>(list to sameList, map to sameMap, data to sameData)) {
            assertEquals(left, right)
            assertEquals("hash of $left and $right", left.hashCode(), right.hashCode())
            assertTrue("set lookup of $right", right in hashSetOf(left))
        }

        assertNotEquals(
            AnyCodable(listOf(AnyCodable(twoPow53 + 1))),
            AnyCodable(listOf(AnyCodable(9.007199254740992E15))),
        )
        assertNotEquals(mapOf("n" to AnyCodable(Long.MAX_VALUE)), mapOf("n" to AnyCodable(twoPow63)))
    }
}
