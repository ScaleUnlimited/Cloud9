/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.ArrayListOfLongs;


/**
 * Writable extension of the ArrayListOfLongs class. This class allows the user to have an efficient 
 * data structure to store a list of longs in MapReduce tasks. It is especially useful for storing 
 * index lists, as it has an efficient intersection method.
 * 
 * @author Ferhan Ture
 *
 */
public class ArrayListOfLongsWritable extends ArrayListOfLongs implements Writable {

	/**
	 * Constructs an ArrayListOfLongsWritable object.
	 */
	public ArrayListOfLongsWritable() {
		super();
	}

	/**
	 * Constructs an ArrayListOfLongsWritable object from a given integer range [ first , last ).
	 * The created list includes the first parameter but excludes the second.
	 * 
	 * @param firstNumber
	 * 					the smallest integer in the range
	 * @param lastNumber
	 * 					the largest integer in the range
	 */
	public ArrayListOfLongsWritable(int firstNumber, int lastNumber) {
		super();
		int j=0;
		for(int i=firstNumber;i<lastNumber;i++){
			this.add(j++, i);
		}	
	}

	/**
	 * Constructs an empty list with the specified initial capacity.
	 * 
	 * @param initialCapacity
	 * 			the initial capacity of the list
	 */
	public ArrayListOfLongsWritable(int initialCapacity) {
		super(initialCapacity);
	}

	/**
	 * Constructs a deep copy of the ArrayListOfLongsWritable object 
	 * given as parameter.
	 * 
	 * @param other
	 * 			object to be copied
	 */
	public ArrayListOfLongsWritable(ArrayListOfLongsWritable other) {
		super();
		for(int i=0;i<other.size();i++){
			add(i, other.get(i));
		}
	}

	public ArrayListOfLongsWritable(long[] perm) {
		super();
		for(int i=0;i<perm.length;i++){
			add(i, perm[i]);
		}	
	}

	/**
	 * Deserializes this object.
	 * 
	 * @param in
	 * 			source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		this.clear();
		int size = in.readInt();
		for(int i=0;i<size;i++){
			add(i,in.readLong());
		}
	}

	/**
	 * Serializes this object.
	 * 
	 * @param out
	 * 			where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		int size = size();
		out.writeInt(size);
		for(int i=0;i<size;i++){
			out.writeLong(get(i));
		}
	}

	/**
	 * Generates human-readable String representation of this ArrayList.
	 * 
	 * @return human-readable String representation of this ArrayList
	 */
	public String toString(){
		if(this==null){
			return "null";
		}
		
		int size = size();
		if(size==0){
			return "[]";
		}

		StringBuilder result = new StringBuilder("[");
		for(int i=0;i<size-1;i++){
			result.append(get(i));
			result.append(',');
		}
		
		result.append(get(size-1));
		result.append(']');
		return result.toString();
	}

	/**
	 * Computes the intersection of two sorted lists of this type.
	 * This method is tuned for efficiency, therefore this ArrayListOfLongsWritable
	 * and the parameter are both assumed to be sorted in an increasing 
	 * order.
	 * 
	 * The ArrayListOfLongsWritable that is returned is the intersection of this object
	 * and the parameter. That is, the returned list will only contain the elements that
	 * occur in both this object and <code>other</code>.
	 * 
	 * @param other
	 * 			other ArrayListOfLongsWritable that is intersected with this object		
	 * @return
	 * 			intersection of <code>other</code> and this object
	 */
	public ArrayListOfLongsWritable intersection(ArrayListOfLongsWritable other) {
		ArrayListOfLongsWritable intersection = new ArrayListOfLongsWritable();
		int len, curPos=0;
		if(size()<other.size()){
			len=size();
			for(int i=0;i<len;i++){
				long elt=this.get(i);
				while(curPos<other.size() && other.get(curPos)<elt){
					curPos++;
				}
				if(curPos>=other.size()){
					return intersection;
				}else if(other.get(curPos)==elt){
					intersection.add(elt);
				}
			}
		}else{
			len=other.size();
			for(int i=0;i<len;i++){
				long elt=other.get(i);
				while(curPos<size() && get(curPos)<elt){
					curPos++;
				}
				if(curPos>=size()){
					return intersection;
				}else if(get(curPos)==elt){
					intersection.add(elt);
				}
			}
		}
		if(intersection.size()==0){
			intersection=null;
		}
		return intersection;
	}
	
	/**
	 * @param start
	 * 	first index to be included in sub-list
	 * @param end
	 * 	last index to be included in sub-list
	 * @return
	 * 	return a new ArrayListOfLongsWritable object, containing the longs of this object from <code>start</code> to <code>end</code>
	 */
	public ArrayListOfLongsWritable sub(int start, int end) {
		ArrayListOfLongsWritable sublst = new ArrayListOfLongsWritable(end-start+1);
		for(int i=start;i<=end;i++){
			sublst.add(get(i));
		}
		return sublst;
	}
	

	/**
	 * Add all longs in the specified array into this object. Check for duplicates.
	 * @param arr
	 * 		array of longs to add to this object
	 */
	public void addAll(long[] arr) {

		for(int i=0;i<arr.length;i++){
			long elt = arr[i];
			if(!contains(elt)){
				add(elt);
			}
		}
	}


}
