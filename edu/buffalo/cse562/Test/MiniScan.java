package edu.buffalo.cse562.Test;

import edu.buffalo.cse562.DTO.Datum;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class MiniScan{
	BufferedReader br;
	BufferedReader[] brs;
	String line;
	TreeMap<Integer, String> typeMap;
	boolean multiple;

	public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws IOException{
		Charset charset = Charset.forName("US-ASCII");
		this.br = Files.newBufferedReader(filename.toPath(), charset);
		this.typeMap = typeMap;
	}

	public MiniScan(BufferedReader finalOutput,
			TreeMap<Integer, String> typeMap2) {
		// TODO Auto-generated constructor stub
		this.br = finalOutput;
		this.typeMap = typeMap2;
	}
	
	private ArrayList<Datum> parseLine(String raw){

		String col[] = line.split("\\|");
		ArrayList<Datum> tuples = new ArrayList<Datum>();
		for(Map.Entry<Integer, String> entry : typeMap.entrySet()) {
			tuples.add(new Datum(entry.getValue(), col[entry.getKey()]));	
		}
		return tuples;
	}
	
	public ArrayList<Datum> readTuple(int index){
		try {
			if ((line = brs[index].readLine())!= null){
				return parseLine(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
		try {
			brs[index].close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;			
	}
	public ArrayList<Datum> readTuple(){
		try {
			if ((line = br.readLine())!= null){
				return parseLine(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
		return null;
	}

	
	public void close(){		
		try {
			this.br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (brs != null){
			if (brs.length > 0){
				for (BufferedReader br : brs){
					try {
						this.br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
		}
	}
}
