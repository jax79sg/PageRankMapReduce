package util;

public final class CustomProperties {
	public static String keySeparatorsStr="**";
	public static String keySeparatorsRegex="\\*\\*";
	public static String valueSeparatorsRegex="\\t";
	public static String valueSeparatorsStr="\t";
	public static boolean printDebug=false;
//	public static float initialPageRank= 1/75879f;
//	public static float initialPageRank= 1/5f;
//	public static float initialPageRank= 1/6f;
	public static int noOfPageRankInteration= 20;
	public static float multiplier=1000000000;
	public static float dampingFactor=0.85f;
	
	public static void printDebug(String message)
	{
		if (CustomProperties.printDebug){
		System.out.println(message);	}
	}
	
	
public static enum PAGERANK_COUNTER{
	MASSLOST,
	TOTALNODES,
	TOTALEDGES,
	MASSGIVENOUT,
};
}
