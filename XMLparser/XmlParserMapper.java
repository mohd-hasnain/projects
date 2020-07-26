package com.aexp.cs.bulletproofing;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
/***
 * 
 * @author Ravi.Mandholia
 *
 */
public class XmlParserMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	String primaryKeys[];
	String start_end_tag;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		// reading primary keys from config file
		primaryKeys = context.getConfiguration().get("primaryKeys").split(",");

		// reading start_end_tag from config file
		start_end_tag = context.getConfiguration().get("start_end_tag");

	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String xmlString = value.toString();
		String out1 = "";

		try {

			// XML dom is created to read xml file
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			DocumentBuilder db = dbf.newDocumentBuilder();
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(xmlString));
			Document doc = db.parse(is);

			Element parNode = (Element) doc.getElementsByTagName(start_end_tag)
					.item(0);

			// by using output schema each tag is read from given xml data file
			// and final output record is created
			for (int i = 0; i < primaryKeys.length; i++) {

				Element elementNode = null;

				if (i == 0) {

					elementNode = (Element) parNode.getElementsByTagName(
							primaryKeys[i]).item(0);

					String attribVal = null;
					if (elementNode != null
							&& elementNode.getTextContent() != null) {
						attribVal = elementNode.getTextContent();

					} else if (elementNode != null
							&& elementNode.getFirstChild() != null
							&& elementNode.getFirstChild().getNodeValue() != null) {
						attribVal = elementNode.getFirstChild().getNodeValue();
					} else {
						attribVal = "";
					}

					out1 = attribVal;
				} else {
					elementNode = (Element) parNode.getElementsByTagName(
							primaryKeys[i]).item(0);

					String attribVal = null;
					if (elementNode != null
							&& elementNode.getTextContent() != null) {
						attribVal = elementNode.getTextContent();

					} else if (elementNode != null
							&& elementNode.getFirstChild() != null
							&& elementNode.getFirstChild().getNodeValue() != null) {
						attribVal = elementNode.getFirstChild().getNodeValue();
					} else {
						attribVal = "";
					}

					out1 = out1 + "_" + attribVal;
				}
			}// end of loop
			context.write(new Text(out1),value);
		} catch (ParserConfigurationException e) {
			
		 e.printStackTrace();
		 throw new IOException(e);
		}
		catch (SAXException e1) {
			e1.printStackTrace();
			throw new IOException(e1);
		}

	}

}
