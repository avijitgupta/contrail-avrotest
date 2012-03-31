#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <fstream>
using namespace std;
int main(int argc, char* argv[])
{
	ifstream ifs(argv[1]);
	FILE* o;
	o = fopen(argv[2],"wb");
	long long int index,num,i, temp[8];
	char ch;
	long long int mask = (1<<8) - 1;
	cout<<mask;
	while(ifs>>index>>num)
	{
		//cout<<index<<"\t"<<num<<endl;
		mask = (1<<8) - 1;
		
		for(i = 0; i < 8; i++)
		{
			temp[i] = (num & mask)>> i*8;
			mask = mask << 8;
			
		}
		for(i = 7; i >= 0; i--)
		{
		fputc(temp[i], o);
			
		}
		
		
	}
	fclose(o);
	ifs.close();
}
