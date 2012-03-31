package org.avrotest;

import java.io.IOException;


/**
 * The node class represents a node in the graph.
 * Each node represents a k-mer and edges are drawn
 * between two nodes if they overlap by k-1 bases.
 * 
 * Each node contains a bunch of fields stored in a hash
 * table. The value of each field is a list of strings.
 * 
 * Each node stores a list of out going edges. The out 
 * going edges are divided into four groups:
 *  "ff", "fr", "rf","rr"
 * where each group corresponds to a different edge type.
 * The edge type is determined by the canonical direction
 * of the kmer (see BuildGraph.BuildGraphMapper.Map)  
 *  
 * 
 */
public class Node  {

	public static String rc(String seq)
	{
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--)
		{
			if      (seq.charAt(i) == 'A') { sb.append('T'); }
			else if (seq.charAt(i) == 'T') { sb.append('A'); }
			else if (seq.charAt(i) == 'C') { sb.append('G'); }
			else if (seq.charAt(i) == 'G') { sb.append('C'); }
		}

		return sb.toString();
	}

	/**
	 * Compare a string to its reverse complement
	 * 
	 * @param seq - The sequence.
	 * @return - 'f' if the DNA sequence precedes is reverse complement
	 * 	lexicographically. 'r' otherwise. 
	 */
	public static char canonicaldir(String seq)
	{
		String rc = rc(seq);
		if (seq.compareTo(rc) < 0)
		{
			return 'f';
		}

		return 'r';
	}

	/**
	 * Returns the canonical version of a DNA sequence.
	 * 
	 * The canonical version of a sequence is the result
	 * of comparing a DNA sequence to its reverse complement
	 * and returning the one which comes first when ordered lexicographically.
	 *  
	 * @param seq
	 * @return - The canonical version of the DNA sequence.
	 */
	public static String canonicalseq(String seq)
	{
		String rc = rc(seq);
		if (seq.compareTo(rc) < 0)
		{
			return seq;
		}

		return rc;
	}


	/**
	 * Return the opposite direction.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	public static String flip_dir(String dir) throws IOException
	{
		if (dir.equals("f")) { return "r"; }
		if (dir.equals("r")) { return "f"; }

		throw new IOException("Unknown dir type: " + dir);
	}

	public static String flip_link(String link) throws IOException
	{
		if (link.equals("ff")) { return "rr"; }
		if (link.equals("fr")) { return "fr"; }
		if (link.equals("rf")) { return "rf"; }
		if (link.equals("rr")) { return "ff"; }

		throw new IOException("Unknown link type: " + link);
	}

	/**
	 * @return The DNA sequence represented by this node as a normal ASCII string. 
	 */
	
}
