package visualization;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Ellipse2D;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;

import clustering.SparkKMeans;


public class DrawingPoints
{
	public static void main(String[] args)
	{


		JFrame f = new JFrame();
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		PointPanel myPanel =new PointPanel();

		f.getContentPane().add(myPanel);
		f.setSize(400,400);

		f.setLocation(200,200);
		f.setVisible(true);
	}
}

class PointPanel extends JPanel
{
	List<Ellipse2D> pointList;
	List<Ellipse2D> ovalList;
	Color selectedColor;
	Ellipse2D selectedPoint;



	public PointPanel()
	{

		final SparkConf conf = new SparkConf().setAppName("K-means Example").setMaster("local");
		final JavaSparkContext  sc = new JavaSparkContext(conf);


		pointList = new ArrayList<Ellipse2D>();
		ovalList = new ArrayList<Ellipse2D>(); 
		selectedColor = Color.red;
		addMouseListener(new PointLocater(this));
		setBackground(Color.white);

		JButton runKmeansButton = new JButton("KMeans");
		JButton cleanButton = new JButton("CLEAN");
		final JTextField inputText = new JTextField("1");

		runKmeansButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int clusterNumber=1;
				try{
					clusterNumber = Integer.parseInt(inputText.getText());
				}catch (Exception exce) {
					exce.printStackTrace();
				}

				ovalList.clear();
				List<Double[]>points =new ArrayList<Double[]>();
				for(Ellipse2D p:pointList ){
					Double [] a=new Double[2];
					a[0] = p.getX();
					a[1] = p.getY();
					points.add(a);
				}

				JavaRDD<Vector> pointData = SparkKMeans.loadPoint(sc, points);

				List<double[]> rs = SparkKMeans.getCenters(sc, pointData, clusterNumber, 10);

				for(double[]point:rs){
					//System.out.println(point[0]+" "+point[1]);
					Ellipse2D e2 = new Ellipse2D.Double(point[0],point[1], 14, 14);
					ovalList.add(e2);
				}
				repaint();
			}          
		});

		cleanButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				pointList.clear();
				ovalList.clear();

				repaint();
			}          
		});

		this.add(runKmeansButton);
		this.add(cleanButton);
		this.add(inputText);
	}

	protected void paintComponent(Graphics g)
	{
		super.paintComponent(g);
		Graphics2D g2 = (Graphics2D)g;
		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
				RenderingHints.VALUE_ANTIALIAS_ON);
		Ellipse2D e;
		Color color;
		for(int j = 0; j < pointList.size(); j++)
		{
			e = (Ellipse2D)pointList.get(j);
			if(e == selectedPoint)
				color = selectedColor;
			else
				color = Color.blue;
			g2.setPaint(color);
			g2.fill(e);
		}

		for(int j = 0; j < ovalList.size(); j++)
		{
			e = (Ellipse2D)ovalList.get(j);
			if(e == selectedPoint)
				color = selectedColor;
			else
				color = Color.RED;
			g2.setPaint(color);
			g2.drawOval((int)e.getX(), (int)e.getY(), 32, 32);
		}


	}

	public List getPointList()
	{
		return pointList;
	}

	public void setSelectedPoint(Ellipse2D e)
	{
		selectedPoint = e;
		repaint();
	}

	public void addPoint(Point p)
	{
		Ellipse2D e = new Ellipse2D.Double(p.x - 3, p.y - 3, 6, 6);
		pointList.add(e);
		selectedPoint = null;
		repaint();
	}
}

class PointLocater extends MouseAdapter
{
	PointPanel pointPanel;

	public PointLocater(PointPanel pp)
	{
		pointPanel = pp;
	}

	public void mousePressed(MouseEvent e)
	{
		Point p = e.getPoint();
		boolean haveSelection = false;
		List list = pointPanel.getPointList();
		Ellipse2D ellipse;
		for(int j = 0; j < list.size(); j++)
		{
			ellipse = (Ellipse2D)list.get(j);
			if(ellipse.contains(p))
			{
				pointPanel.setSelectedPoint(ellipse);
				haveSelection = true;
				break;
			}
		}
		if(!haveSelection)
			pointPanel.addPoint(p);
	}
}
