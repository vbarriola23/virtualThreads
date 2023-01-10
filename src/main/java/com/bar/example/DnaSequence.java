package com.bar.example;

public class DnaSequence {
	
	public String dnaSequence = "";

	public static void main(String[] args) {
		
		DnaSequence sequence = new DnaSequence("ATGACTAGGCTTAAATCGTAAATCATGAACAATGATTGA");
		sequence.getGenes();
	}
	
	public DnaSequence(String dnaSequence) {
		
		this.dnaSequence = dnaSequence;
	}
	
	public void getGenes() {
		
		int ATGIndex = 0;
		System.out.println("There are the following genes in this dna sequence:");
		
		while((ATGIndex = dnaSequence.indexOf("ATG", ATGIndex)) != -1) {
			
			int taaIndex = findClosestEnding("TAA", ATGIndex);
			if (taaIndex == -1)
				taaIndex = Integer.MAX_VALUE;
			int tagIndex = findClosestEnding("TAG", ATGIndex);
			if (tagIndex == -1)
				tagIndex = Integer.MAX_VALUE;
			int tgaIndex = findClosestEnding("TGA", ATGIndex);
			if (tgaIndex == -1)
				tgaIndex = Integer.MAX_VALUE;
			
			if (taaIndex == Integer.MAX_VALUE && tagIndex == Integer.MAX_VALUE && tgaIndex == Integer.MAX_VALUE)
				break;
		
			findGene(ATGIndex, taaIndex, tagIndex, tgaIndex);

			ATGIndex += 3;
		}
	}
	
	//label can be: TAA, TAG or TGA, IF not found it returns -1
	public int findClosestEnding(String label, int initIndex) {
		
		int endIndex = initIndex;
		while((endIndex = dnaSequence.indexOf(label, endIndex + 3)) != -1) {
			int length =  endIndex - initIndex;
			if (length % 3 == 0)
				return endIndex;
		}
		return -1;
	}
	
	public void findGene(int ATGIndex, int taaIndex, int tagIndex, int tgaIndex) {
		
		if (taaIndex < tagIndex && taaIndex < tgaIndex) 
			System.out.println("There is a gene: " + dnaSequence.substring(ATGIndex, taaIndex + 3));
		else if (tagIndex < tgaIndex)
			System.out.println("There is a gene: " + dnaSequence.substring(ATGIndex, tagIndex + 3));
		else
			System.out.println("There is a gene: " + dnaSequence.substring(ATGIndex, tgaIndex + 3));
	}
}
